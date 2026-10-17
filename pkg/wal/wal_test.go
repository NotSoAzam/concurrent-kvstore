package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWAL_WriteAndReplay(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "test.wal")
	w, err := Open(walPath, true)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	// Write entries
	if err := w.WritePut("user:1001", `{"name":"Alice","role":"admin"}`); err != nil {
		t.Fatalf("failed write put: %v", err)
	}
	if err := w.WritePut("user:1002", `{"name":"Bob","role":"member"}`); err != nil {
		t.Fatalf("failed write put: %v", err)
	}
	if err := w.WriteDelete("user:1001"); err != nil {
		t.Fatalf("failed write del: %v", err)
	}

	// Replay before close
	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	if entries[0].Type != OpPut || entries[0].Key != "user:1001" {
		t.Fatalf("unexpected entry 0: %+v", entries[0])
	}
	if entries[1].Type != OpPut || entries[1].Key != "user:1002" {
		t.Fatalf("unexpected entry 1: %+v", entries[1])
	}
	if entries[2].Type != OpDelete || entries[2].Key != "user:1001" {
		t.Fatalf("unexpected entry 2: %+v", entries[2])
	}

	// Close WAL
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close wal: %v", err)
	}

	// Re-open and verify persistence
	w2, err := Open(walPath, false)
	if err != nil {
		t.Fatalf("failed to re-open wal: %v", err)
	}
	defer w2.Close()

	entries2, err := w2.Replay()
	if err != nil {
		t.Fatalf("re-open replay failed: %v", err)
	}
	if len(entries2) != 3 {
		t.Fatalf("expected 3 entries after re-open, got %d", len(entries2))
	}
}

func TestWAL_CorruptionDetection(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_corrupt_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "corrupt.wal")
	w, err := Open(walPath, true)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	_ = w.WritePut("keyA", "valA")
	_ = w.WritePut("keyB", "valB")
	w.Close()

	// Corrupt a byte in the file
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("failed reading file: %v", err)
	}
	// Flip a bit in the body of the second record
	data[len(data)-2] ^= 0xFF
	if err := os.WriteFile(walPath, data, 0644); err != nil {
		t.Fatalf("failed writing corrupted file: %v", err)
	}

	w2, err := Open(walPath, false)
	if err != nil {
		t.Fatalf("failed opening corrupted wal: %v", err)
	}
	defer w2.Close()

	_, err = w2.Replay()
	if err != ErrCorruptEntry {
		t.Fatalf("expected ErrCorruptEntry, got %v", err)
	}
}

func TestWAL_Checkpoint(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_checkpoint_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "checkpoint.wal")
	w, err := Open(walPath, true)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}
	defer w.Close()

	// Write 100 entries with mutations and deletes
	for i := 0; i < 50; i++ {
		_ = w.WritePut("key", "val")
	}

	sizeBefore, _ := w.Size()

	// Compact down to only 1 active key
	live := []Pair{
		{Key: "key", Value: "val_compacted"},
	}
	if err := w.Checkpoint(live); err != nil {
		t.Fatalf("checkpoint failed: %v", err)
	}

	sizeAfter, _ := w.Size()
	if sizeAfter >= sizeBefore {
		t.Fatalf("expected compacted size < before, got after=%d, before=%d", sizeAfter, sizeBefore)
	}

	// Verify replay after checkpoint
	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("replay failed after checkpoint: %v", err)
	}
	if len(entries) != 1 || entries[0].Key != "key" || entries[0].Value != "val_compacted" {
		t.Fatalf("unexpected entries after checkpoint: %+v", entries)
	}
}
