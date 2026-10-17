package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/systems-architect/kvstore/pkg/engine"
)

// Server coordinates the multi-threaded TCP network layer and the storage engine.
type Server struct {
	addr     string
	engine   *engine.Engine
	listener net.Listener
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewServer creates a new server instance.
func NewServer(addr string, eng *engine.Engine) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		addr:   addr,
		engine: eng,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start listens for incoming TCP connections and dispatches each to an independent goroutine.
func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to bind address %s: %w", s.addr, err)
	}

	log.Printf("[Server] Key-Value Store listening on %s (RESP & Line Protocol supported)", s.addr)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return nil // Clean shutdown initiated
			default:
				log.Printf("[Server] Connection accept error: %v", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection reads and processes commands using a buffered reader.
// Supports both RESP (Redis serialization protocol) and plain line-based text commands.
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.wg.Done()
	}()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		// Set read deadline to periodically check context cancellation
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

		// Peek first byte to determine protocol
		firstByte, err := reader.Peek(1)
		if err != nil {
			if err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
				// Normal client disconnect or EOF
			}
			return
		}

		var args []string
		isRESP := false

		if firstByte[0] == '*' {
			// RESP Array format: *<argc>\r\n$<len>\r\n<arg>\r\n...
			isRESP = true
			args, err = readRESPArray(reader)
			if err != nil {
				return
			}
		} else {
			// Inline plain-text command (e.g. "SET foo bar\r\n" or "GET foo\r\n")
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if len(line) == 0 {
				continue
			}
			args = parseCommandLine(line)
		}

		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])
		if err := s.dispatchCommand(cmd, args[1:], writer, isRESP); err != nil {
			log.Printf("[Server] Error dispatching command %s: %v", cmd, err)
			return
		}

		if err := writer.Flush(); err != nil {
			return
		}
	}
}

// dispatchCommand routes parsed command arguments to the appropriate engine function.
func (s *Server) dispatchCommand(cmd string, args []string, w *bufio.Writer, isRESP bool) error {
	switch cmd {
	case "PING":
		if isRESP {
			_, err := w.WriteString("+PONG\r\n")
			return err
		}
		_, err := w.WriteString("PONG\r\n")
		return err

	case "SET":
		if len(args) < 2 {
			return writeError(w, "ERR wrong number of arguments for 'set' command", isRESP)
		}
		key := args[0]
		val := strings.Join(args[1:], " ")
		if err := s.engine.Put(key, val); err != nil {
			return writeError(w, fmt.Sprintf("ERR %v", err), isRESP)
		}
		if isRESP {
			_, err := w.WriteString("+OK\r\n")
			return err
		}
		_, err := w.WriteString("OK\r\n")
		return err

	case "GET":
		if len(args) != 1 {
			return writeError(w, "ERR wrong number of arguments for 'get' command", isRESP)
		}
		key := args[0]
		val, found, err := s.engine.Get(key)
		if err != nil {
			return writeError(w, fmt.Sprintf("ERR %v", err), isRESP)
		}
		if !found {
			if isRESP {
				_, err := w.WriteString("$-1\r\n")
				return err
			}
			_, err := w.WriteString("(nil)\r\n")
			return err
		}
		if isRESP {
			_, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(val), val)
			return err
		}
		_, err = fmt.Fprintf(w, "%s\r\n", val)
		return err

	case "DEL":
		if len(args) != 1 {
			return writeError(w, "ERR wrong number of arguments for 'del' command", isRESP)
		}
		key := args[0]
		if err := s.engine.Delete(key); err != nil {
			return writeError(w, fmt.Sprintf("ERR %v", err), isRESP)
		}
		if isRESP {
			_, err := w.WriteString(":1\r\n")
			return err
		}
		_, err := w.WriteString("DELETED\r\n")
		return err

	case "SCAN":
		if len(args) < 1 {
			return writeError(w, "ERR wrong number of arguments for 'scan' command (usage: SCAN <start> [end])", isRESP)
		}
		startKey := args[0]
		endKey := ""
		if len(args) >= 2 {
			endKey = args[1]
		}

		pairs, err := s.engine.Scan(startKey, endKey)
		if err != nil {
			return writeError(w, fmt.Sprintf("ERR %v", err), isRESP)
		}

		if isRESP {
			// Return array of formatted strings
			_, _ = fmt.Fprintf(w, "*%d\r\n", len(pairs)*2)
			for _, p := range pairs {
				_, _ = fmt.Fprintf(w, "$%d\r\n%s\r\n", len(p.Key), p.Key)
				_, _ = fmt.Fprintf(w, "$%d\r\n%s\r\n", len(p.Value), p.Value)
			}
			return nil
		}

		_, _ = fmt.Fprintf(w, "OK %d items\r\n", len(pairs))
		for _, p := range pairs {
			_, _ = fmt.Fprintf(w, "%s -> %s\r\n", p.Key, p.Value)
		}
		return nil

	case "CHECKPOINT":
		if err := s.engine.Checkpoint(); err != nil {
			return writeError(w, fmt.Sprintf("ERR checkpoint failed: %v", err), isRESP)
		}
		if isRESP {
			_, err := w.WriteString("+OK\r\n")
			return err
		}
		_, err := w.WriteString("OK Checkpoint completed\r\n")
		return err

	case "STATS":
		stats := s.engine.Stats()
		data, _ := json.MarshalIndent(stats, "", "  ")
		if isRESP {
			_, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(data), string(data))
			return err
		}
		_, err := fmt.Fprintf(w, "%s\r\n", string(data))
		return err

	default:
		return writeError(w, fmt.Sprintf("ERR unknown command '%s'", cmd), isRESP)
	}
}

// readRESPArray parses standard Redis RESP arrays (*<argc>\r\n$<len>\r\n<arg>\r\n).
func readRESPArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) < 2 || line[0] != '*' {
		return nil, fmt.Errorf("invalid resp array header: %s", line)
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid resp array length: %w", err)
	}

	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		lenLine, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		lenLine = strings.TrimRight(lenLine, "\r\n")
		if len(lenLine) < 2 || lenLine[0] != '$' {
			return nil, fmt.Errorf("invalid resp bulk string header: %s", lenLine)
		}

		strLen, err := strconv.Atoi(lenLine[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid resp bulk length: %w", err)
		}

		buf := make([]byte, strLen+2) // +2 for trailing \r\n
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:strLen]))
	}

	return args, nil
}

// parseCommandLine splits simple line input honoring spaces.
func parseCommandLine(line string) []string {
	var parts []string
	var current strings.Builder
	inQuotes := false

	for i := 0; i < len(line); i++ {
		ch := line[i]
		switch ch {
		case '"':
			inQuotes = !inQuotes
		case ' ':
			if inQuotes {
				current.WriteByte(ch)
			} else if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		default:
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

func writeError(w *bufio.Writer, msg string, isRESP bool) error {
	if isRESP {
		_, err := fmt.Fprintf(w, "-%s\r\n", msg)
		return err
	}
	_, err := fmt.Fprintf(w, "ERROR: %s\r\n", msg)
	return err
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() {
	s.cancel()
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
}

func main() {
	port := flag.Int("port", 6379, "TCP server port")
	walPath := flag.String("wal", "data/kvstore.wal", "Path to Write-Ahead Log")
	syncWAL := flag.Bool("sync", false, "Synchronously fsync WAL on every write")
	lruCap := flag.Int("lru", 50000, "LRU Cache capacity in items")
	bloomKeys := flag.Uint64("bloom-keys", 200000, "Bloom Filter estimated capacity")
	flag.Parse()

	opts := engine.DefaultOptions(*walPath)
	opts.SyncWAL = *syncWAL
	opts.LRUCapacity = *lruCap
	opts.BloomExpectedKeys = *bloomKeys

	log.Printf("[Init] Starting Storage Engine (WAL=%s, Sync=%v, LRU=%d, BloomKeys=%d)...",
		*walPath, *syncWAL, *lruCap, *bloomKeys)

	eng, err := engine.New(opts)
	if err != nil {
		log.Fatalf("[Fatal] Failed to initialize engine: %v", err)
	}
	defer func() {
		log.Println("[Shutdown] Flushing WAL and closing storage engine...")
		if err := eng.Close(); err != nil {
			log.Printf("[Error] Engine close error: %v", err)
		}
		log.Println("[Shutdown] Storage engine successfully closed.")
	}()

	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	srv := NewServer(addr, eng)

	// Setup OS signal interception for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("[Signal] Received signal %v. Initiating graceful shutdown...", sig)
		srv.Stop()
	}()

	if err := srv.Start(); err != nil {
		log.Printf("[Server] Listener exited: %v", err)
	}
}
