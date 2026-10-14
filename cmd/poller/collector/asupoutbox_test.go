package collector

import (
	"context"
	"errors"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 10, 9, 8, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type scriptedSender struct {
	mu     sync.Mutex
	calls  map[string]int
	order  []string
	decide func(payloadPath string, attempt int) asupResult
	gate   chan struct{} // non-nil: every fork blocks until the channel is closed
}

func newScriptedSender(decide func(payloadPath string, attempt int) asupResult) *scriptedSender {
	return &scriptedSender{calls: map[string]int{}, decide: decide}
}

func (s *scriptedSender) send(_ context.Context, payloadPath string) asupResult {
	if s.gate != nil {
		<-s.gate
	}
	s.mu.Lock()
	s.calls[payloadPath]++
	attempt := s.calls[payloadPath]
	s.order = append(s.order, payloadPath)
	decide := s.decide
	s.mu.Unlock()
	if decide != nil {
		return decide(payloadPath, attempt)
	}
	return asupResult{status: statusDelivered, exitCode: 0}
}

func (s *scriptedSender) callCount(payloadPath string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[payloadPath]
}

func alwaysDeliver(_ string, _ int) asupResult {
	return asupResult{status: statusDelivered, exitCode: 0}
}

func retryableOnceThenDeliver(_ string, attempt int) asupResult {
	if attempt == 1 {
		return asupResult{status: statusRetryable, exitCode: 1, err: errors.New("boom")}
	}
	return asupResult{status: statusDelivered, exitCode: 0}
}

func testBuilder() PayloadBuilder {
	return func() (*Payload, error) {
		return &Payload{Harvest: &harvestInfo{Version: "test"}}, nil
	}
}

func newTestDeliverer(t *testing.T, clock *fakeClock, sender asupSendFunc) *AsupDeliverer {
	t.Helper()
	root := path.Join(t.TempDir(), "asup")
	d, err := newDeliverer("poller-1", root, sender, clock.now)
	if err != nil {
		t.Fatalf("newDeliverer: %v", err)
	}
	t.Cleanup(d.Close)
	return d
}

func restartDeliverer(t *testing.T, d *AsupDeliverer, clock *fakeClock, sender asupSendFunc) *AsupDeliverer {
	t.Helper()
	restarted, err := newDeliverer(d.poller, d.rootDir, sender, clock.now)
	if err != nil {
		t.Fatalf("restart newDeliverer: %v", err)
	}
	t.Cleanup(restarted.Close)
	return restarted
}

func countFiles(t *testing.T, dir, prefix string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", dir, err)
	}
	n := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) {
			n++
		}
	}
	return n
}

func TestEnqueueGivesStableIdentityAndNeverOverwrites(t *testing.T) {
	clock := newFakeClock()
	d := newTestDeliverer(t, clock, newScriptedSender(alwaysDeliver).send)

	if err := d.Enqueue(TriggerSchedule, false, testBuilder()); err != nil {
		t.Fatalf("enqueue 1: %v", err)
	}
	clock.advance(time.Minute)
	if err := d.Enqueue(TriggerSchedule, false, testBuilder()); err != nil {
		t.Fatalf("enqueue 2: %v", err)
	}

	d.mu.Lock()
	n := len(d.entries)
	id1, id2 := d.entries[0].ID, d.entries[1].ID
	p1, p2 := d.entries[0].PayloadPath, d.entries[1].PayloadPath
	d.mu.Unlock()

	if n != 2 || id1 == id2 || p1 == p2 {
		t.Fatalf("expected two distinct entries, got n=%d id1=%s id2=%s", n, id1, id2)
	}
	if got := countFiles(t, d.payloadDir, "poller-1-"); got != 2 {
		t.Fatalf("expected 2 payload files on disk, got %d", got)
	}

	// state survives a restart with both entries still pending
	restarted := restartDeliverer(t, d, clock, newScriptedSender(alwaysDeliver).send)
	restarted.mu.Lock()
	n = len(restarted.entries)
	restarted.mu.Unlock()
	if n != 2 {
		t.Fatalf("expected 2 recovered entries, got %d", n)
	}
}

func TestFirstWriteIsHeldUntilSendCycle(t *testing.T) {
	clock := newFakeClock()
	sender := newScriptedSender(alwaysDeliver)
	d := newTestDeliverer(t, clock, sender.send)

	if err := d.Enqueue(TriggerFirstWrite, true, testBuilder()); err != nil {
		t.Fatalf("enqueue inspection: %v", err)
	}

	// a flush (background retry) must not deliver the inspection payload
	d.Flush()
	if got := len(sender.order); got != 0 {
		t.Fatalf("inspection payload must not be forked, sidecar calls=%d", got)
	}
	d.mu.Lock()
	state := d.entries[0].State
	d.mu.Unlock()
	if state != entryInspection {
		t.Fatalf("expected inspection state, got %s", state)
	}

	// send cycle: inspection admitted first, fresh payload delivered after it
	if err := d.SendDue(TriggerSchedule, testBuilder()); err != nil {
		t.Fatalf("SendDue: %v", err)
	}
	if got := len(sender.order); got != 2 {
		t.Fatalf("expected 2 sidecar calls, got %d", got)
	}
	d.mu.Lock()
	remaining := len(d.entries)
	d.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("expected empty outbox, got %d entries", remaining)
	}
	if got := countFiles(t, d.payloadDir, "poller-1-"); got != 0 {
		t.Fatalf("acknowledged payload files must be removed, got %d", got)
	}
}

func TestRetryableFailureIsRetriedAndSurvivesRestart(t *testing.T) {
	clock := newFakeClock()
	sender := newScriptedSender(retryableOnceThenDeliver)
	d := newTestDeliverer(t, clock, sender.send)

	if err := d.Enqueue(TriggerSchedule, false, testBuilder()); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	var payloadPath string
	d.mu.Lock()
	payloadPath = d.entries[0].PayloadPath
	d.mu.Unlock()

	d.Flush()
	if sender.callCount(payloadPath) != 1 {
		t.Fatalf("expected one sidecar attempt, got %d", sender.callCount(payloadPath))
	}
	d.mu.Lock()
	e := d.entries[0]
	state, nextAttempt := e.State, e.NextAttemptAt
	d.mu.Unlock()
	if state != entryPending {
		t.Fatalf("expected pending state after retryable failure, got %s", state)
	}
	if !nextAttempt.After(clock.now()) {
		t.Fatalf("nextAttempt %v must be in the future (now %v)", nextAttempt, clock.now())
	}
	if _, err := os.Stat(payloadPath); err != nil {
		t.Fatalf("unacknowledged payload must remain on disk: %v", err)
	}

	// not due yet: no new attempt
	d.Flush()
	if sender.callCount(payloadPath) != 1 {
		t.Fatalf("payload must not be retried before backoff elapsed, calls=%d", sender.callCount(payloadPath))
	}

	// restart: unacknowledged payload is recovered and retried with backoff
	clock.advance(maxRetryBackoff)
	restarted := restartDeliverer(t, d, clock, newScriptedSender(retryableOnceThenDeliver).send)
	restarted.Flush()
	if _, err := os.Stat(payloadPath); err != nil {
		t.Fatal("unacknowledged payload must be retained when the first post-restart attempt fails")
	}
	clock.advance(maxRetryBackoff)
	restarted.Flush()
	if _, err := os.Stat(payloadPath); !os.IsNotExist(err) {
		t.Fatal("payload must be removed after acknowledgement across restart")
	}
	restarted.mu.Lock()
	remaining := len(restarted.entries)
	restarted.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("expected empty outbox after restart delivery, got %d", remaining)
	}
}

func TestSidecarMissingIsDistinguishableAndDoesNotBlockLaterPayloads(t *testing.T) {
	clock := newFakeClock()
	missing := newScriptedSender(func(_ string, _ int) asupResult {
		return asupResult{status: statusUnavailable, exitCode: -1, err: errors.New("not found")}
	})
	d := newTestDeliverer(t, clock, missing.send)

	for range 3 {
		if err := d.Enqueue(TriggerSchedule, false, testBuilder()); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	d.Flush()

	d.mu.Lock()
	for _, e := range d.entries {
		if e.State != entrySidecarMissing {
			t.Fatalf("expected sidecar-missing state, got %s for %s", e.State, e.ID)
		}
		if !e.NextAttemptAt.After(clock.now()) {
			t.Fatalf("missing entry %s must back off", e.ID)
		}
	}
	d.mu.Unlock()
	// only one fork attempt when the sidecar is known missing during a flush
	if total := len(missing.order); total != 1 {
		t.Fatalf("expected exactly 1 fork when sidecar missing, got %d", total)
	}
	if got := countFiles(t, d.payloadDir, "poller-1-"); got != 3 {
		t.Fatalf("all 3 payload files must be retained, got %d", got)
	}

	// sidecar installed: entries are retried and delivered, newest blocked never happened
	clock.advance(missingRetryBackoff + time.Second)
	ok := newScriptedSender(alwaysDeliver)
	restarted := restartDeliverer(t, d, clock, ok.send)
	restarted.Flush()
	if len(ok.order) != 3 {
		t.Fatalf("expected all 3 payloads delivered after sidecar recovered, got %d", len(ok.order))
	}
	if got := countFiles(t, restarted.payloadDir, "poller-1-"); got != 0 {
		t.Fatalf("expected 0 retained files, got %d", got)
	}
}

func TestPermanentFailureQuarantinesEntryAndKeepsDeliveringOthers(t *testing.T) {
	clock := newFakeClock()
	var firstPath string
	sender := newScriptedSender(func(p string, _ int) asupResult {
		if p == firstPath {
			return asupResult{status: statusPermanent, exitCode: asupExitPermanent, err: errors.New("rejected")}
		}
		return asupResult{status: statusDelivered, exitCode: 0}
	})
	d := newTestDeliverer(t, clock, sender.send)

	if err := d.Enqueue(TriggerSchedule, false, testBuilder()); err != nil {
		t.Fatalf("enqueue 1: %v", err)
	}
	d.mu.Lock()
	firstPath = d.entries[0].PayloadPath
	d.mu.Unlock()
	clock.advance(time.Minute)
	if err := d.Enqueue(TriggerSchedule, false, testBuilder()); err != nil {
		t.Fatalf("enqueue 2: %v", err)
	}

	d.Flush()
	d.Flush() // failed entry must be skipped, never forked again

	if sender.callCount(firstPath) != 1 {
		t.Fatalf("permanently failed payload must be attempted once, got %d", sender.callCount(firstPath))
	}
	d.mu.Lock()
	if len(d.entries) != 1 || d.entries[0].State != entryFailed {
		t.Fatalf("expected single failed entry, got %+v", d.entries)
	}
	d.mu.Unlock()
	if _, err := os.Stat(firstPath); err != nil {
		t.Fatal("permanently failed payload file must be retained for inspection")
	}

	// state persists across restart
	restarted := restartDeliverer(t, d, clock, sender.send)
	restarted.Flush()
	if sender.callCount(firstPath) != 1 {
		t.Fatal("failed entry must not be retried after restart")
	}
	restarted.mu.Lock()
	if len(restarted.entries) != 1 || restarted.entries[0].State != entryFailed {
		t.Fatalf("failed state must survive restart, got %+v", restarted.entries)
	}
	restarted.mu.Unlock()
}

func TestCorruptStateIsQuarantinedAndOrphansAdopted(t *testing.T) {
	clock := newFakeClock()
	root := path.Join(t.TempDir(), "asup")

	// seed a first deliverer with one pending payload, then corrupt its state
	seed, err := newDeliverer("poller-1", root, newScriptedSender(alwaysDeliver).send, clock.now)
	if err != nil {
		t.Fatalf("seed deliverer: %v", err)
	}
	if err := seed.Enqueue(TriggerSchedule, false, testBuilder()); err != nil {
		t.Fatalf("seed enqueue: %v", err)
	}
	payloadFiles := countFiles(t, seed.payloadDir, "poller-1-")
	if payloadFiles != 1 {
		t.Fatalf("expected 1 seeded payload, got %d", payloadFiles)
	}
	if err := os.WriteFile(seed.statePath, []byte("{not-json"), filePerm); err != nil {
		t.Fatalf("corrupt state: %v", err)
	}

	sender := newScriptedSender(alwaysDeliver)
	d, err := newDeliverer("poller-1", root, sender.send, clock.now)
	if err != nil {
		t.Fatalf("recover deliverer: %v", err)
	}
	t.Cleanup(d.Close)

	// corrupted state preserved alongside a fresh valid state file
	dir, _ := os.ReadDir(d.outboxDir)
	quarantined := 0
	for _, f := range dir {
		if strings.Contains(f.Name(), ".corrupt-") {
			quarantined++
		}
	}
	if quarantined != 1 {
		t.Fatalf("expected 1 quarantined state file, got %d", quarantined)
	}
	d.mu.Lock()
	if len(d.entries) != 1 || d.entries[0].State != entryPending {
		t.Fatalf("expected orphan payload adopted as pending, got %+v", d.entries)
	}
	d.mu.Unlock()

	d.Flush()
	if len(sender.order) != 1 {
		t.Fatalf("adopted payload must be deliverable, calls=%d", len(sender.order))
	}
}

func TestOnlyOwnPollerPayloadsAreAdopted(t *testing.T) {
	clock := newFakeClock()
	root := path.Join(t.TempDir(), "asup")
	first, err := newDeliverer("poller-1", root, newScriptedSender(alwaysDeliver).send, clock.now)
	if err != nil {
		t.Fatalf("first deliverer: %v", err)
	}
	if err := first.Enqueue(TriggerSchedule, false, testBuilder()); err != nil {
		t.Fatalf("enqueue poller-1: %v", err)
	}

	// payload from another poller and a leftover temp file must be ignored
	other := path.Join(first.payloadDir, "poller-2-20260101T000000Z-0000.json")
	if err := os.WriteFile(other, []byte("{}"), filePerm); err != nil {
		t.Fatalf("write other payload: %v", err)
	}
	tmp := path.Join(first.payloadDir, "poller-1-whatever.json.tmp")
	if err := os.WriteFile(tmp, []byte("partial"), filePerm); err != nil {
		t.Fatalf("write tmp: %v", err)
	}

	d, err := newDeliverer("poller-1", root, newScriptedSender(alwaysDeliver).send, clock.now)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	t.Cleanup(d.Close)
	d.mu.Lock()
	got := len(d.entries)
	d.mu.Unlock()
	if got != 1 {
		t.Fatalf("expected only 1 owned payload adopted, got %d", got)
	}
}

func TestLegacyPayloadIsAdoptedAsInspection(t *testing.T) {
	clock := newFakeClock()
	root := path.Join(t.TempDir(), "asup")
	first, err := newDeliverer("poller-1", root, newScriptedSender(alwaysDeliver).send, clock.now)
	if err != nil {
		t.Fatalf("first deliverer: %v", err)
	}
	legacyPath := path.Join(first.payloadDir, "poller-1_payload.json")
	if err := os.WriteFile(legacyPath, []byte("{}"), filePerm); err != nil {
		t.Fatalf("seed legacy payload: %v", err)
	}

	sender := newScriptedSender(alwaysDeliver)
	d, err := newDeliverer("poller-1", root, sender.send, clock.now)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	t.Cleanup(d.Close)

	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatal("legacy fixed-name payload must be renamed to a stable identity")
	}
	d.mu.Lock()
	if len(d.entries) != 1 || d.entries[0].State != entryInspection || d.entries[0].Trigger != triggerLegacy {
		t.Fatalf("expected legacy entry held as inspection, got %+v", d.entries)
	}
	d.mu.Unlock()

	d.Flush()
	if len(sender.order) != 0 {
		t.Fatal("legacy inspection payload must not be delivered before a send cycle")
	}
	if err := d.SendDue(TriggerSchedule, testBuilder()); err != nil {
		t.Fatalf("SendDue: %v", err)
	}
	if len(sender.order) != 2 {
		t.Fatalf("expected legacy + fresh payload delivered, got %d", len(sender.order))
	}
}

func TestPayloadWriteFailureDoesNotBlockLaterPayloads(t *testing.T) {
	clock := newFakeClock()
	d := newTestDeliverer(t, clock, newScriptedSender(alwaysDeliver).send)

	// turn the payload directory into a regular file: writes must fail
	if err := os.RemoveAll(d.payloadDir); err != nil {
		t.Fatalf("remove payload dir: %v", err)
	}
	if err := os.WriteFile(d.payloadDir, []byte("x"), filePerm); err != nil {
		t.Fatalf("block payload dir: %v", err)
	}

	if err := d.Enqueue(TriggerSchedule, false, testBuilder()); err == nil {
		t.Fatal("expected enqueue to fail when payload directory is unusable")
	}
	d.mu.Lock()
	entries := len(d.entries)
	d.mu.Unlock()
	if entries != 0 {
		t.Fatalf("failed write must not record an entry, got %d", entries)
	}

	// repair the disk and verify a later payload flows through
	if err := os.Remove(d.payloadDir); err != nil {
		t.Fatalf("repair: %v", err)
	}
	if err := os.MkdirAll(d.payloadDir, dirPerm); err != nil {
		t.Fatalf("recreate payload dir: %v", err)
	}
	if err := d.Enqueue(TriggerSchedule, false, testBuilder()); err != nil {
		t.Fatalf("enqueue after repair: %v", err)
	}
	d.Flush()
	if got := countFiles(t, d.payloadDir, "poller-1-"); got != 0 {
		t.Fatalf("repaired-flow payload should be acknowledged and removed, got %d", got)
	}
}

func TestOutboxStateWriteFailureIsRecoveredFromPayloadFiles(t *testing.T) {
	clock := newFakeClock()
	root := path.Join(t.TempDir(), "asup")
	d, err := newDeliverer("poller-1", root, newScriptedSender(alwaysDeliver).send, clock.now)
	if err != nil {
		t.Fatalf("new deliverer: %v", err)
	}
	t.Cleanup(d.Close)

	// block the outbox directory so state cannot be persisted
	if err := os.RemoveAll(d.outboxDir); err != nil {
		t.Fatalf("remove outbox dir: %v", err)
	}
	if err := os.WriteFile(d.outboxDir, []byte("x"), filePerm); err != nil {
		t.Fatalf("block outbox dir: %v", err)
	}
	if err := d.Enqueue(TriggerSchedule, false, testBuilder()); err == nil {
		t.Fatal("expected state persist failure")
	}
	if got := countFiles(t, d.payloadDir, "poller-1-"); got != 1 {
		t.Fatalf("payload itself must be durable despite state failure, got %d", got)
	}

	// repair, restart: orphaned payload is re-adopted and delivered
	if err := os.Remove(d.outboxDir); err != nil {
		t.Fatalf("repair: %v", err)
	}
	sender := newScriptedSender(alwaysDeliver)
	recovered, err := newDeliverer("poller-1", root, sender.send, clock.now)
	if err != nil {
		t.Fatalf("restart deliverer: %v", err)
	}
	t.Cleanup(recovered.Close)
	recovered.Flush()
	if len(sender.order) != 1 {
		t.Fatalf("expected recovered payload delivered, calls=%d", len(sender.order))
	}
}

func TestConcurrentFlushesForkSidecarOncePerPayload(t *testing.T) {
	clock := newFakeClock()
	sender := newScriptedSender(alwaysDeliver)
	release := make(chan struct{})
	sender.gate = release // every fork blocks here until all flushers are queued
	d := newTestDeliverer(t, clock, sender.send)

	if err := d.Enqueue(TriggerSchedule, false, testBuilder()); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	var payloadPath string
	d.mu.Lock()
	payloadPath = d.entries[0].PayloadPath
	d.mu.Unlock()

	var arrived atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			arrived.Add(1)
			d.Flush()
		}()
	}
	for arrived.Load() != 32 {
		runtime.Gosched()
	}
	close(release)
	wg.Wait()

	if got := sender.callCount(payloadPath); got != 1 {
		t.Fatalf("payload must be submitted exactly once, sidecar calls=%d", got)
	}
}

func TestSendDueDeliversPendingEvenWhenFreshBuildFails(t *testing.T) {
	clock := newFakeClock()
	sender := newScriptedSender(alwaysDeliver)
	d := newTestDeliverer(t, clock, sender.send)

	if err := d.Enqueue(TriggerSchedule, false, testBuilder()); err != nil {
		t.Fatalf("enqueue pending: %v", err)
	}

	failingBuild := func() (*Payload, error) {
		return nil, errors.New("build failed")
	}
	if err := d.SendDue(TriggerSchedule, failingBuild); err == nil {
		t.Fatal("SendDue must surface the build error")
	}
	if len(sender.order) != 1 {
		t.Fatalf("pending payload must still be delivered when build fails, calls=%d", len(sender.order))
	}
}

func TestRetryBackoffIsCapped(t *testing.T) {
	cases := map[int]time.Duration{
		1:   initialRetryBackoff,
		2:   2 * initialRetryBackoff,
		3:   4 * initialRetryBackoff,
		100: maxRetryBackoff,
	}
	for attempts, want := range cases {
		if got := retryBackoff(attempts); got != want {
			t.Errorf("retryBackoff(%d) = %s, want %s", attempts, got, want)
		}
	}
}

func TestSafePollerName(t *testing.T) {
	cases := map[string]string{
		"poller-1":    "poller-1",
		"a/b:c":       "a_b_c",
		"":            "poller",
		".":           "poller",
		"..":          "poller",
		"UPPER_1.2-3": "UPPER_1.2-3",
	}
	for in, want := range cases {
		if got := safePollerName(in); got != want {
			t.Errorf("safePollerName(%q) = %q, want %q", in, got, want)
		}
	}
}
