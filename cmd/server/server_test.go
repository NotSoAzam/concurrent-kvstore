package main

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/systems-architect/kvstore/pkg/engine"
)

func TestTCPServer_RESPAndLineCommands(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tcp_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "server_test.wal")
	eng, err := engine.New(engine.DefaultOptions(walPath))
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer eng.Close()

	// Find random available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind test listener: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	srv := NewServer(addr, eng)
	go func() {
		_ = srv.Start()
	}()
	defer srv.Stop()

	// Wait for server to bind
	time.Sleep(50 * time.Millisecond)

	// Connect TCP client
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// 1. Test Plain-text SET
	_, err = conn.Write([]byte("SET user:1001 {\"name\":\"Alice\"}\r\n"))
	if err != nil {
		t.Fatalf("failed writing SET: %v", err)
	}
	resp, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(resp, "OK") {
		t.Fatalf("unexpected SET response: %s (err=%v)", resp, err)
	}

	// 2. Test Plain-text GET
	_, err = conn.Write([]byte("GET user:1001\r\n"))
	if err != nil {
		t.Fatalf("failed writing GET: %v", err)
	}
	resp, err = reader.ReadString('\n')
	if err != nil || !strings.Contains(resp, "Alice") {
		t.Fatalf("unexpected GET response: %s (err=%v)", resp, err)
	}

	// 3. Test RESP Protocol SET
	// *3\r\n$3\r\nSET\r\n$9\r\nuser:1002\r\n$5\r\n{\"id\":2}\r\n
	respCmd := "*3\r\n$3\r\nSET\r\n$9\r\nuser:1002\r\n$8\r\n{\"id\":2}\r\n"
	_, err = conn.Write([]byte(respCmd))
	if err != nil {
		t.Fatalf("failed writing RESP SET: %v", err)
	}
	resp, err = reader.ReadString('\n')
	if err != nil || strings.TrimSpace(resp) != "+OK" {
		t.Fatalf("unexpected RESP SET response: %s (err=%v)", resp, err)
	}

	// 4. Test RESP Protocol GET
	getCmd := "*2\r\n$3\r\nGET\r\n$9\r\nuser:1002\r\n"
	_, err = conn.Write([]byte(getCmd))
	if err != nil {
		t.Fatalf("failed writing RESP GET: %v", err)
	}
	respLen, _ := reader.ReadString('\n')
	respVal, _ := reader.ReadString('\n')
	if !strings.HasPrefix(respLen, "$8") || !strings.Contains(respVal, "{\"id\":2}") {
		t.Fatalf("unexpected RESP GET response: len=%s, val=%s", respLen, respVal)
	}

	// 5. Test SCAN
	_, err = conn.Write([]byte("SCAN user:0000 user:9999\r\n"))
	if err != nil {
		t.Fatalf("failed writing SCAN: %v", err)
	}
	scanHeader, _ := reader.ReadString('\n')
	if !strings.Contains(scanHeader, "OK 2 items") {
		t.Fatalf("unexpected SCAN header: %s", scanHeader)
	}
	_, _ = reader.ReadString('\n') // read item 1
	_, _ = reader.ReadString('\n') // read item 2

	// 6. Test DEL
	_, err = conn.Write([]byte("DEL user:1001\r\n"))
	if err != nil {
		t.Fatalf("failed writing DEL: %v", err)
	}
	delResp, _ := reader.ReadString('\n')
	if !strings.Contains(delResp, "DELETED") {
		t.Fatalf("unexpected DEL response: %s", delResp)
	}
}
