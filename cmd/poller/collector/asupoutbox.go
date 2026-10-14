package collector

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/netapp/harvest/v2/pkg/logging"
	"github.com/netapp/harvest/v2/pkg/slogx"
)

// AsupDeliverer durably delivers autosupport payloads to the asup sidecar
// binary. Every fully built payload is written to a uniquely named file and
// recorded in an on-disk outbox before delivery is attempted. An entry is only
// removed after the sidecar acknowledges (exit 0). Failures are classified,
// recorded as distinguishable states and retried with backoff. Both the
// payload files and the outbox survive process restarts.
type AsupDeliverer struct {
	mu         sync.Mutex
	poller     string
	rootDir    string // e.g. "asup"
	payloadDir string // e.g. "asup/payload"
	outboxDir  string // e.g. "asup/outbox"
	statePath  string // e.g. "asup/outbox/poller.json"
	entries    []*asupEntry
	send       asupSendFunc
	now        func() time.Time
	closeCh    chan struct{}
}

// PayloadBuilder assembles a fresh autosupport payload. It is invoked by the
// deliverer immediately before the payload is persisted and enqueued.
type PayloadBuilder func() (*Payload, error)

// Lifecycle states of an outbox entry.
const (
	// entryInspection: payload was written so its contents can be examined
	// (first-write trigger). The sidecar is never forked for an inspection
	// entry until the next send cycle admits it.
	entryInspection = "inspection"
	// entryPending: payload is waiting to be delivered or for a retry timer.
	entryPending = "pending"
	// entrySidecarMissing: the last delivery attempt could not find the
	// sidecar binary. Retried later, independently of other payloads.
	entrySidecarMissing = "sidecar-missing"
	// entryFailed: sidecar rejected the payload with a non-retryable exit
	// code. The payload is retained for inspection but never retried.
	entryFailed = "failed"
)

const (
	outboxVersion                   = 1
	payloadDirName                  = "payload"
	outboxDirName                   = "outbox"
	legacyPayloadSuffix             = "_payload.json"
	tempPayloadSuffix               = ".tmp"
	dirPerm             os.FileMode = 0o750
	filePerm            os.FileMode = 0o600

	asupCallTimeout = 10 * time.Second

	// Exit code contract of the asup sidecar:
	//   0 -> payload accepted
	//   2 -> permanent rejection (bad payload, configuration ...), do not retry
	// any other non-zero exit / signal / timeout is treated as transient.
	asupExitOK        = 0
	asupExitPermanent = 2

	retryFlushInterval  = 30 * time.Second
	initialRetryBackoff = 1 * time.Minute
	maxRetryBackoff     = 1 * time.Hour
	missingRetryBackoff = 5 * time.Minute
)

// Trigger sources recorded on outbox entries.
const (
	// TriggerFirstWrite: payload written for inspection shortly after start-up.
	TriggerFirstWrite = "first-write"
	// TriggerSchedule: payload created by a scheduled/startup send cycle.
	TriggerSchedule  = "schedule"
	triggerRecovered = "recovered"
	triggerLegacy    = "legacy"
)

type asupEntry struct {
	ID            string    `json:"id"`
	State         string    `json:"state"`
	Trigger       string    `json:"trigger"`
	PayloadPath   string    `json:"payloadPath"`
	Attempts      int       `json:"attempts"`
	CreatedAt     time.Time `json:"createdAt"`
	LastAttemptAt time.Time `json:"lastAttemptAt,omitempty"`
	NextAttemptAt time.Time `json:"nextAttemptAt,omitempty"`
	LastError     string    `json:"lastError,omitempty"`
	ExitCode      int       `json:"exitCode,omitempty"`
}

type asupOutboxState struct {
	Version int          `json:"version"`
	Poller  string       `json:"poller"`
	Entries []*asupEntry `json:"entries"`
}

type asupStatus int

const (
	statusDelivered asupStatus = iota
	statusRetryable
	statusPermanent
	statusUnavailable
)

type asupResult struct {
	status   asupStatus
	exitCode int
	err      error
}

// asupSendFunc forks the sidecar for the given payload. Injected in tests.
type asupSendFunc func(ctx context.Context, payloadPath string) asupResult

// default sidecar locations, tried in order
var asupBinaryPaths = []string{
	"./autosupport/asup",
	"../harvest-private/harvest-asup/bin/asup",
}

// NewAsupDeliverer creates (or recovers) the outbox for pollerName and starts
// the background retry loop. A recovered outbox re-adopts payload files that
// are not referenced by the state file; a corrupted state file is quarantined
// instead of blocking delivery.
func NewAsupDeliverer(pollerName string) (*AsupDeliverer, error) {
	d, err := newDeliverer(pollerName, workingDir, forkAsupSidecar, time.Now)
	if err != nil {
		return nil, err
	}
	go d.flushLoop()
	return d, nil
}

// newDeliverer builds a deliverer rooted at rootDir without starting the retry
// loop. Used directly by tests.
func newDeliverer(pollerName, rootDir string, send asupSendFunc, now func() time.Time) (*AsupDeliverer, error) {
	if send == nil {
		send = forkAsupSidecar
	}
	if now == nil {
		now = time.Now
	}
	d := &AsupDeliverer{
		poller:  pollerName,
		rootDir: rootDir,
		send:    send,
		now:     now,
		closeCh: make(chan struct{}),
	}
	d.payloadDir = path.Join(rootDir, payloadDirName)
	d.outboxDir = path.Join(rootDir, outboxDirName)
	d.statePath = path.Join(d.outboxDir, safePollerName(pollerName)+".json")

	if err := d.ensureDirs(); err != nil {
		return nil, err
	}
	d.load()
	return d, nil
}

func (d *AsupDeliverer) ensureDirs() error {
	for _, dir := range []string{d.rootDir, d.payloadDir, d.outboxDir} {
		if err := checkAndDeleteIfPermissionsMismatch(dir, dirPerm); err != nil {
			logging.Get().Warn("", slogx.Err(err), slog.String("dir", dir))
		}
		if err := os.MkdirAll(dir, dirPerm); err != nil {
			return fmt.Errorf("could not create autosupport directory %s: %w", dir, err)
		}
	}
	return nil
}

// load reads the persisted outbox, quarantining it when corrupted, drops
// entries whose payload no longer exists and adopts orphan payload files.
func (d *AsupDeliverer) load() {
	entries := d.readState()
	d.entries = entries

	known := make(map[string]bool, len(entries))
	kept := entries[:0]
	for _, e := range entries {
		if e == nil || e.ID == "" || e.PayloadPath == "" {
			slog.Default().Warn(
				"dropping malformed autosupport outbox entry",
				slog.String("poller", d.poller),
			)
			continue
		}
		if _, err := os.Stat(e.PayloadPath); err != nil {
			slog.Default().Warn(
				"dropping autosupport outbox entry, payload file missing",
				slogx.Err(err),
				slog.String("poller", d.poller),
				slog.String("id", e.ID),
				slog.String("payloadPath", e.PayloadPath),
			)
			continue
		}
		known[e.PayloadPath] = true
		kept = append(kept, e)
	}
	d.entries = kept

	d.adoptOrphanPayloads(known)

	if err := d.persistLocked(); err != nil {
		slog.Default().Error(
			"failed to persist recovered autosupport outbox",
			slogx.Err(err),
			slog.String("poller", d.poller),
		)
	}
}

func (d *AsupDeliverer) readState() []*asupEntry {
	data, err := os.ReadFile(d.statePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		slog.Default().Error(
			"failed to read autosupport outbox state, starting with empty outbox",
			slogx.Err(err),
			slog.String("poller", d.poller),
			slog.String("statePath", d.statePath),
		)
		return nil
	}

	var state asupOutboxState
	if err := json.Unmarshal(data, &state); err != nil {
		d.quarantineState(data, err)
		return nil
	}
	if state.Version != outboxVersion {
		d.quarantineState(data, fmt.Errorf("unsupported autosupport outbox version %d", state.Version))
		return nil
	}
	if state.Poller != d.poller {
		d.quarantineState(data, fmt.Errorf("autosupport outbox belongs to poller %q", state.Poller))
		return nil
	}
	return state.Entries
}

// quarantineState moves an unreadable state file aside so it can neither block
// delivery nor be silently overwritten.
func (d *AsupDeliverer) quarantineState(data []byte, cause error) {
	quarantined := fmt.Sprintf("%s.corrupt-%d", d.statePath, d.now().UnixNano())
	if err := os.WriteFile(quarantined, data, filePerm); err != nil {
		slog.Default().Error(
			"failed to preserve corrupted autosupport outbox state",
			slogx.Err(err),
			slog.String("poller", d.poller),
			slog.String("quarantinePath", quarantined),
		)
	}
	if err := os.Remove(d.statePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		slog.Default().Error(
			"failed to remove corrupted autosupport outbox state",
			slogx.Err(err),
			slog.String("poller", d.poller),
			slog.String("statePath", d.statePath),
		)
	}
	slog.Default().Error(
		"autosupport outbox state corrupted, quarantined and recovering from payload files",
		slogx.Err(cause),
		slog.String("poller", d.poller),
		slog.String("statePath", d.statePath),
		slog.String("quarantinePath", quarantined),
	)
}

// adoptOrphanPayloads re-enqueues payload files present on disk but absent from
// the state, plus the legacy single {poller}_payload.json file. They are
// unacknowledged by definition and must not be lost.
func (d *AsupDeliverer) adoptOrphanPayloads(known map[string]bool) {
	safeName := safePollerName(d.poller)

	matches, err := os.ReadDir(d.payloadDir)
	if err != nil {
		slog.Default().Warn(
			"failed to scan autosupport payload directory, skipping recovery",
			slogx.Err(err),
			slog.String("poller", d.poller),
			slog.String("payloadDir", d.payloadDir),
		)
		return
	}

	for _, m := range matches {
		if m.IsDir() {
			continue
		}
		name := m.Name()
		if strings.HasSuffix(name, tempPayloadSuffix) {
			continue
		}
		fullPath := path.Join(d.payloadDir, name)
		if known[fullPath] {
			continue
		}

		trigger := triggerRecovered
		state := entryPending
		id := strings.TrimSuffix(name, ".json")

		// legacy fixed-name payload written by older Harvest releases
		if name == d.poller+legacyPayloadSuffix || name == safeName+legacyPayloadSuffix {
			newID := d.newIDLocked()
			newPath := path.Join(d.payloadDir, newID+".json")
			if err := os.Rename(fullPath, newPath); err != nil {
				slog.Default().Warn(
					"failed to adopt legacy autosupport payload, leaving file in place",
					slogx.Err(err),
					slog.String("poller", d.poller),
					slog.String("payloadPath", fullPath),
				)
				continue
			}
			fullPath = newPath
			id = newID
			trigger = triggerLegacy
			state = entryInspection
			slog.Default().Info(
				"adopted legacy autosupport payload",
				slog.String("poller", d.poller),
				slog.String("id", newID),
			)
		} else if !strings.HasPrefix(name, safeName+"-") {
			// payload owned by another poller
			continue
		}

		known[fullPath] = true
		modTime := d.now()
		if info, err := m.Info(); err == nil {
			modTime = info.ModTime()
		}
		d.entries = append(d.entries, &asupEntry{
			ID:          id,
			State:       state,
			Trigger:     trigger,
			PayloadPath: fullPath,
			CreatedAt:   modTime,
		})
		slog.Default().Warn(
			"re-adopted unacknowledged autosupport payload",
			slog.String("poller", d.poller),
			slog.String("id", id),
			slog.String("trigger", trigger),
		)
	}
}

// Enqueue builds, atomically writes and records a new payload. inspection
// entries (first-write) are withheld from delivery until the next send cycle.
// A write failure leaves the outbox untouched and does not block later
// payloads.
func (d *AsupDeliverer) Enqueue(trigger string, inspection bool, build PayloadBuilder) error {
	msg, err := build()
	if err != nil {
		return fmt.Errorf("failed to build autosupport payload poller:%s: %w", d.poller, err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	id := d.newIDLocked()
	payloadPath := path.Join(d.payloadDir, id+".json")
	if err := d.writePayloadLocked(payloadPath, msg); err != nil {
		slog.Default().Error(
			"autosupport payload write failed",
			slogx.Err(err),
			slog.String("poller", d.poller),
			slog.String("id", id),
			slog.String("payloadPath", payloadPath),
		)
		return err
	}

	state := entryPending
	if inspection {
		state = entryInspection
	}
	e := &asupEntry{
		ID:          id,
		State:       state,
		Trigger:     trigger,
		PayloadPath: payloadPath,
		CreatedAt:   d.now(),
	}
	d.entries = append(d.entries, e)

	if err := d.persistLocked(); err != nil {
		slog.Default().Error(
			"autosupport outbox state write failed, payload retained on disk",
			slogx.Err(err),
			slog.String("poller", d.poller),
			slog.String("id", id),
		)
		// keep the in-memory entry; the payload file itself is durable and will
		// be re-adopted on restart if the state cannot be flushed.
		return err
	}

	slog.Default().Info(
		"autosupport payload enqueued",
		slog.String("poller", d.poller),
		slog.String("id", id),
		slog.String("state", state),
		slog.String("trigger", trigger),
		slog.String("payloadPath", payloadPath),
	)
	return nil
}

// writePayloadLocked writes the payload to a temp file, fsyncs it and renames
// it into place so a reader can never observe a partially written payload.
func (d *AsupDeliverer) writePayloadLocked(payloadPath string, msg *Payload) error {
	tmpPath := payloadPath + tempPayloadSuffix
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, filePerm)
	if err != nil {
		return fmt.Errorf("autosupport failed to open %s: %w", tmpPath, err)
	}
	committed := false
	defer func() {
		_ = file.Close()
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", " ")
	if err := encoder.Encode(msg); err != nil {
		return fmt.Errorf("autosupport failed to encode %s: %w", tmpPath, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("autosupport failed to sync %s: %w", tmpPath, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("autosupport failed to close %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, payloadPath); err != nil {
		return fmt.Errorf("autosupport failed to finalize %s: %w", payloadPath, err)
	}
	committed = true
	return nil
}

// SendDue runs a send cycle: inspection entries are admitted, a fresh payload
// is built/enqueued and all due entries are delivered. Pending entries are
// flushed even when building the fresh payload fails.
func (d *AsupDeliverer) SendDue(trigger string, build PayloadBuilder) error {
	d.mu.Lock()
	for _, e := range d.entries {
		if e.State == entryInspection {
			e.State = entryPending
			slog.Default().Info(
				"autosupport payload admitted for delivery",
				slog.String("poller", d.poller),
				slog.String("id", e.ID),
			)
		}
	}
	_ = d.persistLocked()
	d.mu.Unlock()

	buildErr := d.Enqueue(trigger, false, build)
	if buildErr != nil {
		slog.Default().Error(
			"failed to build scheduled autosupport payload, delivering pending payloads",
			slogx.Err(buildErr),
			slog.String("poller", d.poller),
		)
	}

	d.Flush()
	return buildErr
}

// Flush attempts delivery of every entry that is currently due. It is safe to
// call concurrently from any trigger source: all state changes are serialized
// and each entry is forked at most once per flush.
func (d *AsupDeliverer) Flush() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.flushLocked()
}

func (d *AsupDeliverer) flushLocked() {
	now := d.now()
	sidecarKnownMissing := false

	// snapshot indexes; the slice is mutated only through removeEntryLocked
	for i := 0; i < len(d.entries); i++ {
		e := d.entries[i]
		if !d.isDueLocked(e, now) {
			continue
		}

		if sidecarKnownMissing {
			d.markUnavailableLocked(e, errors.New("autosupport sidecar binary not found"))
			_ = d.persistLocked()
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), asupCallTimeout)
		slog.Default().Info(
			"forking autosupport binary",
			slog.String("poller", d.poller),
			slog.String("id", e.ID),
			slog.String("payloadPath", e.PayloadPath),
			slog.Int("attempt", e.Attempts+1),
		)
		result := d.send(ctx, e.PayloadPath)
		cancel()
		e.LastAttemptAt = now
		e.Attempts++
		e.ExitCode = result.exitCode

		switch result.status {
		case statusDelivered:
			slog.Default().Info(
				"autosupport payload acknowledged",
				slog.String("poller", d.poller),
				slog.String("id", e.ID),
				slog.Int("attempts", e.Attempts),
			)
			d.removeEntryLocked(i, e)
			i--
		case statusPermanent:
			e.State = entryFailed
			e.LastError = result.err.Error()
			slog.Default().Error(
				"autosupport payload rejected permanently, no further retries",
				slogx.Err(result.err),
				slog.String("poller", d.poller),
				slog.String("id", e.ID),
				slog.String("payloadPath", e.PayloadPath),
				slog.Int("exitCode", result.exitCode),
			)
		case statusUnavailable:
			sidecarKnownMissing = true
			d.markUnavailableLocked(e, result.err)
		default: // statusRetryable
			e.State = entryPending
			e.NextAttemptAt = now.Add(retryBackoff(e.Attempts))
			if result.err != nil {
				e.LastError = result.err.Error()
			}
			slog.Default().Warn(
				"autosupport delivery failed, will retry",
				slogx.Err(result.err),
				slog.String("poller", d.poller),
				slog.String("id", e.ID),
				slog.Int("attempts", e.Attempts),
				slog.Int("exitCode", result.exitCode),
				slog.Time("nextAttemptAt", e.NextAttemptAt),
			)
		}
		_ = d.persistLocked()
	}
}

func (d *AsupDeliverer) isDueLocked(e *asupEntry, now time.Time) bool {
	switch e.State {
	case entryPending, entrySidecarMissing:
		return !e.NextAttemptAt.After(now)
	default:
		return false
	}
}

func (d *AsupDeliverer) markUnavailableLocked(e *asupEntry, err error) {
	e.State = entrySidecarMissing
	e.NextAttemptAt = d.now().Add(missingRetryBackoff)
	if err != nil {
		e.LastError = err.Error()
	}
	slog.Default().Warn(
		"autosupport sidecar binary not found, payload retained",
		slogx.Err(err),
		slog.String("poller", d.poller),
		slog.String("id", e.ID),
		slog.Time("nextAttemptAt", e.NextAttemptAt),
	)
}

func (d *AsupDeliverer) removeEntryLocked(i int, e *asupEntry) {
	d.entries = append(d.entries[:i], d.entries[i+1:]...)
	if err := os.Remove(e.PayloadPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		// the sidecar acknowledged the payload, so a cleanup failure must not
		// resurrect the entry and cause a duplicate delivery
		slog.Default().Warn(
			"failed to remove acknowledged autosupport payload file",
			slogx.Err(err),
			slog.String("poller", d.poller),
			slog.String("id", e.ID),
			slog.String("payloadPath", e.PayloadPath),
		)
	}
}

func (d *AsupDeliverer) flushLoop() {
	ticker := time.NewTicker(retryFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.closeCh:
			return
		case <-ticker.C:
			d.Flush()
		}
	}
}

// Close stops the background retry loop.
func (d *AsupDeliverer) Close() {
	close(d.closeCh)
}

// persistLocked atomically writes the outbox state.
func (d *AsupDeliverer) persistLocked() error {
	state := asupOutboxState{
		Version: outboxVersion,
		Poller:  d.poller,
		Entries: d.entries,
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("autosupport failed to encode outbox state: %w", err)
	}
	tmpPath := d.statePath + tempPayloadSuffix
	if err := os.WriteFile(tmpPath, data, filePerm); err != nil {
		return fmt.Errorf("autosupport failed to write outbox state %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, d.statePath); err != nil {
		return fmt.Errorf("autosupport failed to finalize outbox state %s: %w", d.statePath, err)
	}
	return nil
}

func (d *AsupDeliverer) newIDLocked() string {
	ts := d.now().UTC().Format("20060102T150405Z")
	buf := make([]byte, 2)
	if _, err := crand.Read(buf); err != nil {
		// crypto/rand failing is extraordinary; time-based suffix keeps IDs unique
		return fmt.Sprintf("%s-%d", safePollerName(d.poller)+"-"+ts, d.now().UnixNano())
	}
	return fmt.Sprintf("%s-%s-%s", safePollerName(d.poller), ts, hex.EncodeToString(buf))
}

func retryBackoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	backoff := initialRetryBackoff
	for i := 1; i < attempts && backoff < maxRetryBackoff; i++ {
		backoff *= 2
	}
	if backoff > maxRetryBackoff {
		return maxRetryBackoff
	}
	return backoff
}

// safePollerName turns a poller name into something usable as a filename
// component.
func safePollerName(name string) string {
	mapped := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			return r
		default:
			return '_'
		}
	}, name)
	if mapped == "" || mapped == "." || mapped == ".." {
		return "poller"
	}
	return mapped
}

// forkAsupSidecar invokes the autosupport binary, falling back to the secondary
// install location when the primary binary is missing. The caller is
// responsible for the deadline on ctx.
func forkAsupSidecar(ctx context.Context, payloadPath string) asupResult {
	var last asupResult
	for _, binaryPath := range asupBinaryPaths {
		err := exec.CommandContext(ctx, binaryPath, "--payload", payloadPath, "--working-dir", workingDir).Run() //nolint:gosec

		if err == nil {
			return asupResult{status: statusDelivered, exitCode: asupExitOK}
		}

		if errors.Is(err, fs.ErrNotExist) {
			last = asupResult{status: statusUnavailable, exitCode: -1, err: fmt.Errorf("autosupport binary not found: %s", binaryPath)}
			continue
		}

		exitCode := -1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		}

		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return asupResult{
				status:   statusRetryable,
				exitCode: exitCode,
				err:      fmt.Errorf("autosupport call to %s timed out after %s: %w", binaryPath, asupCallTimeout, err),
			}
		}

		status := statusRetryable
		if exitCode == asupExitPermanent {
			status = statusPermanent
		}
		return asupResult{
			status:   status,
			exitCode: exitCode,
			err:      fmt.Errorf("autosupport call to %s failed: %w", binaryPath, err),
		}
	}
	if last.err == nil {
		last = asupResult{status: statusUnavailable, exitCode: -1, err: errors.New("autosupport sidecar binary not found")}
	}
	return last
}
