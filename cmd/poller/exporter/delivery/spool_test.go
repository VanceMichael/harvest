/*
Copyright NetApp Inc, 2021 All rights reserved
*/

package delivery

import (
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestSpoolWriteReadAck(t *testing.T) {
	dir := t.TempDir()
	s, recovered, err := NewSpool(dir, slog.Default())
	if err != nil {
		t.Fatalf("NewSpool: %v", err)
	}
	if len(recovered) != 0 {
		t.Fatalf("empty spool recovered %d batches", len(recovered))
	}

	if _, err := s.Write(7, []byte("payload-7")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := s.Read(7)
	if err != nil || string(got) != "payload-7" {
		t.Fatalf("Read = %q, %v", got, err)
	}
	if err := s.Ack(7); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if _, err := s.Read(7); !os.IsNotExist(err) {
		t.Fatalf("expected NotExist after ack, got %v", err)
	}
	// Ack is idempotent.
	if err := s.Ack(7); err != nil {
		t.Fatalf("second Ack: %v", err)
	}
}

func TestSpoolRecoveryAndTmpCleanup(t *testing.T) {
	dir := t.TempDir()

	// Simulate leftovers from a crashed process: two batches and one
	// half-written temp file.
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000002.batch"), []byte("b2"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000001.batch"), []byte("b1"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000003.batch.tmp"), []byte("partial"), 0o640); err != nil {
		t.Fatal(err)
	}

	s, recovered, err := NewSpool(dir, slog.Default())
	if err != nil {
		t.Fatalf("NewSpool: %v", err)
	}
	want := []uint64{1, 2}
	if !slices.Equal(recovered, want) {
		t.Fatalf("recovered = %v, want %v (FIFO by filename)", recovered, want)
	}
	if _, err := os.Stat(filepath.Join(dir, "00000000000000000003.batch.tmp")); !os.IsNotExist(err) {
		t.Fatalf("tmp file not removed: %v", err)
	}
	if s.RejectedCount() != 0 {
		t.Fatalf("unexpected rejected count")
	}
}

func TestSpoolRejectQuarantineAndPrune(t *testing.T) {
	dir := t.TempDir()
	s, _, err := NewSpool(dir, slog.Default())
	if err != nil {
		t.Fatal(err)
	}

	for i := uint64(1); i <= 40; i++ {
		if _, err := s.Write(i, []byte("x")); err != nil {
			t.Fatal(err)
		}
		s.Reject(i)
	}

	if c := s.RejectedCount(); c != maxRejectedLog {
		t.Fatalf("rejected kept = %d, want %d", c, maxRejectedLog)
	}
	// newest are retained
	if _, err := os.Stat(filepath.Join(dir, rejectedDir, batchName(40))); err != nil {
		t.Fatalf("newest rejected batch missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, rejectedDir, batchName(1))); !os.IsNotExist(err) {
		t.Fatalf("oldest rejected batch should have been pruned")
	}
	// rejected batches are no longer pending in the spool root
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() != rejectedDir && e.Type()&fs.ModeDir == 0 {
			t.Fatalf("unexpected file in spool root: %s", e.Name())
		}
	}
}

func TestBatchNameOrdering(t *testing.T) {
	// lexical ordering of filenames must equal numeric FIFO order
	ids := []uint64{1, 9, 10, 99, 100, 123456789}
	names := make([]string, len(ids))
	for i, id := range ids {
		names[i] = batchName(id)
	}
	if !slices.IsSorted(names) {
		t.Fatalf("batch names not sorted: %v", names)
	}
	for i, id := range ids {
		got, ok := parseBatchID(names[i])
		if !ok || got != id {
			t.Fatalf("roundtrip id=%d got=%d ok=%v", id, got, ok)
		}
	}
}

func TestSpoolDirSafeNames(t *testing.T) {
	got := SpoolDir("/tmp/spool", "p 1", "ex/port:er")
	want := filepath.Join("/tmp/spool", "p_1", "ex_port_er")
	if got != want {
		t.Fatalf("SpoolDir = %q, want %q", got, want)
	}
}
