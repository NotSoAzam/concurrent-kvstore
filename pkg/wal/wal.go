package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	// Entry types
	OpPut        byte = 1
	OpDelete     byte = 2
	OpCheckpoint byte = 3

	// Header layout:
	// [CRC32: 4 bytes] [Type: 1 byte] [KeyLen: 4 bytes] [ValueLen: 4 bytes] = 13 bytes
	headerSize = 13
)

var (
	// ErrCorruptEntry is returned when CRC checksum validation fails during replay.
	ErrCorruptEntry = errors.New("wal: data corruption detected, crc checksum mismatch")
	// ErrClosed is returned when an operation is performed on a closed WAL.
	ErrClosed = errors.New("wal: log is closed")
)

// Pair represents a live key-value pair for snapshotting.
type Pair struct {
	Key   string
	Value string
}

// LogEntry represents an entry read from the WAL.
type LogEntry struct {
	Type  byte
	Key   string
	Value string
}

// WAL provides a durable, append-only Write-Ahead Log with CRC32 integrity verification,
// startup replay recovery, and checkpoint compaction.
type WAL struct {
	mu         sync.Mutex
	path       string
	file       *os.File
	syncWrites bool
	closed     bool
}

// Open initializes or re-opens a WAL file at the given path.
func Open(path string, syncWrites bool) (*WAL, error) {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create wal directory: %w", err)
		}
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	return &WAL{
		path:       path,
		file:       file,
		syncWrites: syncWrites,
		closed:     false,
	}, nil
}

// writeEntry formats, checksums, and appends a single entry to the WAL file.
// Format: [CRC32 Checksum (4B) | Type (1B) | KeyLen (4B) | ValueLen (4B) | Key | Value]
func (w *WAL) writeEntry(opType byte, key, value string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}

	keyBytes := []byte(key)
	valBytes := []byte(value)
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(valBytes))

	// Construct payload: [Type | KeyLen | ValueLen | Key | Value]
	payloadSize := 1 + 4 + 4 + len(keyBytes) + len(valBytes)
	payload := make([]byte, payloadSize)

	payload[0] = opType
	binary.BigEndian.PutUint32(payload[1:5], keyLen)
	binary.BigEndian.PutUint32(payload[5:9], valLen)
	copy(payload[9:9+keyLen], keyBytes)
	copy(payload[9+keyLen:], valBytes)

	// Compute CRC32 checksum over payload
	checksum := crc32.ChecksumIEEE(payload)

	// Combine header + payload
	record := make([]byte, 4+payloadSize)
	binary.BigEndian.PutUint32(record[0:4], checksum)
	copy(record[4:], payload)

	if _, err := w.file.Write(record); err != nil {
		return fmt.Errorf("failed to write wal record: %w", err)
	}

	if w.syncWrites {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync wal file: %w", err)
		}
	}

	return nil
}

// WritePut writes an insertion or update operation to the WAL.
func (w *WAL) WritePut(key, value string) error {
	return w.writeEntry(OpPut, key, value)
}

// WriteDelete writes a deletion (tombstone) operation to the WAL.
func (w *WAL) WriteDelete(key string) error {
	return w.writeEntry(OpDelete, key, "")
}

// Sync commits the current file contents to stable storage.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}
	return w.file.Sync()
}

// Replay reads all valid records sequentially from the WAL from the beginning of the file.
// Validates CRC32 checksum for every record. Returns an ordered slice of operations to restore state.
func (w *WAL) Replay() ([]LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, ErrClosed
	}

	// Seek to the beginning of the WAL file
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek wal file: %w", err)
	}

	var entries []LogEntry
	headerBuf := make([]byte, headerSize)

	for {
		_, err := io.ReadFull(w.file, headerBuf)
		if err == io.EOF {
			// Clean end of file reached
			break
		}
		if err == io.ErrUnexpectedEOF {
			// Incomplete header at end of file (crash during write)
			return nil, ErrCorruptEntry
		}
		if err != nil {
			return nil, fmt.Errorf("error reading wal header: %w", err)
		}

		expectedChecksum := binary.BigEndian.Uint32(headerBuf[0:4])
		opType := headerBuf[4]
		keyLen := binary.BigEndian.Uint32(headerBuf[5:9])
		valLen := binary.BigEndian.Uint32(headerBuf[9:13])

		bodySize := int(keyLen + valLen)
		bodyBuf := make([]byte, bodySize)
		if _, err := io.ReadFull(w.file, bodyBuf); err != nil {
			return nil, ErrCorruptEntry
		}

		// Recompute CRC: payload is [Type (1B) | KeyLen (4B) | ValueLen (4B) | Body]
		payload := make([]byte, 9+bodySize)
		copy(payload[0:9], headerBuf[4:13])
		copy(payload[9:], bodyBuf)

		actualChecksum := crc32.ChecksumIEEE(payload)
		if actualChecksum != expectedChecksum {
			return nil, ErrCorruptEntry
		}

		key := string(bodyBuf[:keyLen])
		val := string(bodyBuf[keyLen:])

		entries = append(entries, LogEntry{
			Type:  opType,
			Key:   key,
			Value: val,
		})
	}

	// Seek back to end for subsequent writes
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("failed to seek to wal end: %w", err)
	}

	return entries, nil
}

// Checkpoint compacts the WAL by taking a snapshot of live data and truncating previous log history.
// It writes live entries to a temporary file, atomically replaces the active WAL file, and reopens it.
func (w *WAL) Checkpoint(liveEntries []Pair) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}

	tmpPath := w.path + ".checkpoint.tmp"

	// Create and write live entries to temporary file
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to create temporary checkpoint file: %w", err)
	}

	for _, entry := range liveEntries {
		keyBytes := []byte(entry.Key)
		valBytes := []byte(entry.Value)
		keyLen := uint32(len(keyBytes))
		valLen := uint32(len(valBytes))

		payloadSize := 1 + 4 + 4 + len(keyBytes) + len(valBytes)
		payload := make([]byte, payloadSize)

		payload[0] = OpPut
		binary.BigEndian.PutUint32(payload[1:5], keyLen)
		binary.BigEndian.PutUint32(payload[5:9], valLen)
		copy(payload[9:9+keyLen], keyBytes)
		copy(payload[9+keyLen:], valBytes)

		checksum := crc32.ChecksumIEEE(payload)
		record := make([]byte, 4+payloadSize)
		binary.BigEndian.PutUint32(record[0:4], checksum)
		copy(record[4:], payload)

		if _, err := tmpFile.Write(record); err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("failed to write checkpoint entry: %w", err)
		}
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to sync checkpoint file: %w", err)
	}
	tmpFile.Close()

	// Close the current active WAL file so Windows can safely replace/rename it
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close wal file before swap: %w", err)
	}

	// Atomically replace active WAL with checkpoint file
	// On Windows, if destination exists, os.Rename fails; we remove it first
	_ = os.Remove(w.path)
	if err := os.Rename(tmpPath, w.path); err != nil {
		return fmt.Errorf("failed to replace wal with checkpoint: %w", err)
	}

	// Reopen active WAL file in append mode
	newFile, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen wal file after checkpoint: %w", err)
	}

	w.file = newFile
	return nil
}

// Size returns the current size in bytes of the WAL file.
func (w *WAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, ErrClosed
	}

	stat, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

// Close closes the underlying WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true
	return w.file.Close()
}
