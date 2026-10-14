/*
Copyright NetApp Inc, 2021 All rights reserved

Package delivery implements the durable delivery pipeline between a
collector snapshot and a push exporter remote (InfluxDB, VictoriaMetrics,
...).

A rendered batch is persisted to a per-exporter spool directory *before*
the enqueue call returns. Batches are removed only after the remote
acknowledges them, so network failures, remote 5xx and process restarts
cannot silently drop collection rounds.
*/
package delivery

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/netapp/harvest/v2/pkg/slogx"
	"log/slog"
)

const (
	batchSuffix    = ".batch"
	tmpSuffix      = ".batch.tmp"
	rejectedDir    = "rejected"
	maxRejectedLog = 32
)

// Spool is a directory of unacknowledged batches. File layout:
//
//	<dir>/<zero-padded-sequence>.batch         acknowledged only by deletion
//	<dir>/<zero-padded-sequence>.batch.tmp     partial write (cleaned on start)
//	<dir>/rejected/<seq>.batch                 permanently rejected payloads
//
// The zero-padded sequence makes lexical name order identical to the FIFO
// delivery order.
type Spool struct {
	dir    string
	logger *slog.Logger
	mu     sync.Mutex
}

// NewSpool opens (creating if needed) the spool directory. Batches left over
// from a previous process are returned in FIFO order; partial .tmp files
// are discarded.
func NewSpool(dir string, logger *slog.Logger) (*Spool, []uint64, error) {
	s := &Spool{dir: dir, logger: logger}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, nil, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}

	var recovered []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		switch {
		case strings.HasSuffix(name, tmpSuffix):
			// A process died while writing: the batch was never visible,
			// so it is safe to delete.
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				logger.Warn("unable to remove stale spool temp file", slog.String("file", name), slogx.Err(err))
			}
		case strings.HasSuffix(name, batchSuffix):
			if id, ok := parseBatchID(name); ok {
				recovered = append(recovered, id)
			} else {
				logger.Warn("ignoring unrecognized spool file", slog.String("file", name))
			}
		}
	}
	slices.Sort(recovered)
	return s, recovered, nil
}

// Dir returns the spool directory.
func (s *Spool) Dir() string { return s.dir }

// Write persists one batch atomically (temp file + fsync + rename) and
// returns its path.
func (s *Spool) Write(id uint64, payload []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	final := s.path(id)
	tmp := final + ".tmp"

	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return "", err
	}
	_, writeErr := file.Write(payload)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil {
		_ = os.Remove(tmp)
		return "", writeErr
	}
	if syncErr != nil {
		_ = os.Remove(tmp)
		return "", syncErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return "", closeErr
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return final, nil
}

// Read returns the payload of a spooled batch.
func (s *Spool) Read(id uint64) ([]byte, error) {
	return os.ReadFile(s.path(id))
}

// Ack deletes an acknowledged batch. A missing file is not an error:
// the batch can only be acked once by the single sender goroutine.
func (s *Spool) Ack(id uint64) error {
	if err := os.Remove(s.path(id)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Reject moves a permanently rejected batch to the rejected/ sub-directory
// and prunes the oldest kept payloads.
func (s *Spool) Reject(id uint64) {
	rejectDir := filepath.Join(s.dir, rejectedDir)
	if err := os.MkdirAll(rejectDir, 0o750); err != nil {
		s.logger.Warn("unable to create rejected directory, dropping rejected batch", slogx.Err(err))
		_ = s.Ack(id)
		return
	}

	src := s.path(id)
	dst := filepath.Join(rejectDir, batchName(id))
	if err := os.Rename(src, dst); err != nil && !os.IsNotExist(err) {
		// Cross-device or permission problems: fall back to copy-free drop.
		s.logger.Warn("unable to quarantine rejected batch, dropping it", slogx.Err(err))
		_ = s.Ack(id)
	}
	s.pruneRejected(rejectDir)
}

// pruneRejected keeps at most maxRejectedLog rejected payloads, oldest first.
func (s *Spool) pruneRejected(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), batchSuffix) {
			names = append(names, e.Name())
		}
	}
	slices.Sort(names)
	for len(names) > maxRejectedLog {
		if err := os.Remove(filepath.Join(dir, names[0])); err != nil {
			s.logger.Debug("unable to prune rejected batch", slog.String("file", names[0]), slogx.Err(err))
		}
		names = names[1:]
	}
}

func (s *Spool) path(id uint64) string {
	return filepath.Join(s.dir, batchName(id))
}

func batchName(id uint64) string {
	return fmt.Sprintf("%020d%s", id, batchSuffix)
}

func parseBatchID(name string) (uint64, bool) {
	id, err := strconv.ParseUint(strings.TrimSuffix(name, batchSuffix), 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// RejectedCount reports how many rejected payloads are kept on disk.
func (s *Spool) RejectedCount() int {
	entries, err := os.ReadDir(filepath.Join(s.dir, rejectedDir))
	if err != nil {
		return 0
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), batchSuffix) {
			count++
		}
	}
	return count
}
