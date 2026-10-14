/*
Copyright NetApp Inc, 2021 All rights reserved
*/

package delivery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

// fakePusher is a programmable PushFunc recording calls.
type fakePusher struct {
	mu        sync.Mutex
	sent      []uint64
	payloads  map[uint64]string
	attempts  map[uint64]int
	inFlight  int
	maxFlight int
	failFirst map[uint64]int // batchID -> attempts that fail retryably before success
	rejectIDs map[uint64]bool
	release   <-chan struct{} // when non-nil, pushes block until closed
}

func newFakePusher() *fakePusher {
	return &fakePusher{
		payloads:  map[uint64]string{},
		attempts:  map[uint64]int{},
		failFirst: map[uint64]int{},
		rejectIDs: map[uint64]bool{},
	}
}

func (f *fakePusher) push(_ context.Context, id uint64, payload []byte) error {
	f.mu.Lock()
	f.inFlight++
	if f.inFlight > f.maxFlight {
		f.maxFlight = f.inFlight
	}
	f.attempts[id]++
	attempt := f.attempts[id]
	f.sent = append(f.sent, id)
	f.payloads[id] = string(payload)
	release := f.release
	f.mu.Unlock()

	if release != nil {
		<-release
	}

	f.mu.Lock()
	f.inFlight--
	switch {
	case f.rejectIDs[id]:
		f.mu.Unlock()
		return &Error{kind: ErrRejected, statusCode: 400, inner: errors.New("bad request")}
	case attempt <= f.failFirst[id]:
		f.mu.Unlock()
		return &Error{kind: ErrRetryable, statusCode: 503, inner: errors.New("busy")}
	default:
		f.mu.Unlock()
		return nil
	}
}

func (f *fakePusher) snapshot() ([]uint64, map[uint64]int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.sent), maps.Clone(f.attempts), f.maxFlight
}

type statusSpy struct {
	mu     sync.Mutex
	codes  []uint8
	last   uint8
	reason string
}

func (s *statusSpy) set(code uint8, reason string) {
	s.mu.Lock()
	s.codes = append(s.codes, code)
	s.last = code
	s.reason = reason
	s.mu.Unlock()
}

func (s *statusSpy) lastCode() uint8 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

func (s *statusSpy) saw(code uint8) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Contains(s.codes, code)
}

func newTestQueue(t *testing.T, p PushFunc, spy *statusSpy, opts ...func(*Config)) *Queue {
	t.Helper()
	cfg := Config{
		Poller:          "p1",
		Class:           "InfluxDB",
		Name:            t.Name(),
		Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		Dir:             filepath.Join(t.TempDir(), "spool"),
		QueueMax:        2,
		SpoolMaxBatches: 10,
		RetryMin:        5 * time.Millisecond,
		RetryMax:        50 * time.Millisecond,
		Push:            p,
		SetStatus:       spy.set,
	}
	for _, o := range opts {
		o(&cfg)
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatalf("New queue: %v", err)
	}
	t.Cleanup(q.Close)
	return q
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timed out waiting for: " + msg)
}

// Happy path: batches are pushed in FIFO order, acked files deleted,
// exporter ends up and no errors surface.
func TestQueueFIFOAndAck(t *testing.T) {
	fp := newFakePusher()
	spy := &statusSpy{}
	q := newTestQueue(t, fp.push, spy)
	q.Start()

	for _, id := range []string{"a", "b", "c"} {
		if err := q.Enqueue([]byte("payload-"+id), "obj"); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	waitUntil(t, 2*time.Second, func() bool { return q.Pending() == 0 }, "drain")

	sent, attempts, _ := fp.snapshot()
	if !slices.Equal(sent, []uint64{1, 2, 3}) {
		t.Fatalf("send order = %v, want [1 2 3]", sent)
	}
	for id := uint64(1); id <= 3; id++ {
		if attempts[id] != 1 {
			t.Fatalf("batch %d attempts = %d, want 1", id, attempts[id])
		}
		if fp.payloads[id] != fmt.Sprintf("payload-%c", 'a'+id-1) {
			t.Fatalf("batch %d payload = %q", id, fp.payloads[id])
		}
		if _, err := os.Stat(filepath.Join(q.spool.Dir(), batchName(id))); !os.IsNotExist(err) {
			t.Fatalf("acked batch %d still on disk", id)
		}
	}
	if spy.lastCode() != CodeUp {
		t.Fatalf("final status = %d, want up", spy.lastCode())
	}
	snap := q.Snapshot()
	if snap.Acked != 3 || snap.Enqueued != 3 {
		t.Fatalf("counters = %+v", snap)
	}
}

// Retryable failures keep the batch at the head: it is retried until the
// remote recovers, no data is lost, later batches never overtake it.
func TestQueueRetryThenRecover(t *testing.T) {
	fp := newFakePusher()
	fp.failFirst[1] = 2 // batch 1 fails twice, succeeds on 3rd attempt
	spy := &statusSpy{}
	q := newTestQueue(t, fp.push, spy)
	q.Start()

	if err := q.Enqueue([]byte("p1"), "old"); err != nil {
		t.Fatal(err)
	}
	// enqueue second batch while first is still retrying
	time.Sleep(20 * time.Millisecond)
	if err := q.Enqueue([]byte("p2"), "new"); err != nil {
		t.Fatal(err)
	}

	waitUntil(t, 2*time.Second, func() bool { return q.Pending() == 0 }, "drain")

	sent, attempts, _ := fp.snapshot()
	want := []uint64{1, 1, 1, 2}
	if !slices.Equal(sent, want) {
		t.Fatalf("send sequence = %v, want %v (old retried before new)", sent, want)
	}
	if attempts[2] != 1 {
		t.Fatalf("batch 2 must be attempted once, got %d", attempts[2])
	}
	if !spy.saw(CodeRetrying) {
		t.Fatalf("retrying state never published, codes=%v", spy.codes)
	}
	if spy.lastCode() != CodeUp {
		t.Fatalf("final status = %d, want up after recovery", spy.lastCode())
	}
	if fp.payloads[1] != "p1" || fp.payloads[2] != "p2" {
		t.Fatalf("payloads corrupted: %v", fp.payloads)
	}
}

// Non-retryable 4xx: offending batch is quarantined and dropped from the
// queue; subsequent batches still flow; rejected state is observable then
// clears on the next ack.
func TestQueuePermanentRejection(t *testing.T) {
	fp := newFakePusher()
	fp.rejectIDs[1] = true
	spy := &statusSpy{}
	q := newTestQueue(t, fp.push, spy)
	q.Start()

	if err := q.Enqueue([]byte("bad"), "o1"); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool { return spy.saw(CodeRejected) }, "rejected state")

	if err := q.Enqueue([]byte("good"), "o2"); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 2*time.Second, func() bool { return q.Pending() == 0 }, "drain")

	sent, _, _ := fp.snapshot()
	if !slices.Equal(sent, []uint64{1, 2}) {
		t.Fatalf("send order = %v, want [1 2]", sent)
	}
	// rejected payload quarantined for inspection
	if _, err := os.Stat(filepath.Join(q.spool.Dir(), rejectedDir, batchName(1))); err != nil {
		t.Fatalf("rejected batch not quarantined: %v", err)
	}
	if _, err := os.Stat(filepath.Join(q.spool.Dir(), batchName(1))); !os.IsNotExist(err) {
		t.Fatalf("rejected batch must leave the pending queue")
	}
	snap := q.Snapshot()
	if snap.Rejected != 1 || snap.Acked != 1 {
		t.Fatalf("counters = %+v", snap)
	}
}

// Recovery ordering: batches spooled by a previous process are redelivered
// oldest-first after restart, including payloads that only exist on disk.
func TestQueueRestartRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	spool, _, err := NewSpool(dir, logger)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint64{1, 2, 3} {
		if _, err := spool.Write(id, []byte(fmt.Sprintf("disk-%d", id))); err != nil {
			t.Fatal(err)
		}
	}

	fp := newFakePusher()
	spy := &statusSpy{}
	cfg := Config{
		Poller: "p1", Class: "InfluxDB", Name: "restart",
		Logger: logger, Dir: dir,
		QueueMax: 2, SpoolMaxBatches: 10,
		RetryMin: 5 * time.Millisecond, RetryMax: 50 * time.Millisecond,
		Push: fp.push, SetStatus: spy.set,
	}
	q, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if q.Pending() != 3 {
		t.Fatalf("recovered pending = %d, want 3", q.Pending())
	}
	q.Start()
	t.Cleanup(q.Close)

	if !spy.saw(CodeBacklogged) {
		t.Fatalf("backlogged state not published on recovery")
	}
	waitUntil(t, 2*time.Second, func() bool { return q.Pending() == 0 }, "drain recovered")

	sent, _, _ := fp.snapshot()
	if !slices.Equal(sent, []uint64{1, 2, 3}) {
		t.Fatalf("recovery send order = %v", sent)
	}
	for id := uint64(1); id <= 3; id++ {
		if fp.payloads[id] != fmt.Sprintf("disk-%d", id) {
			t.Fatalf("recovered payload %d = %q", id, fp.payloads[id])
		}
	}
}

// Bounded backlog: beyond the hard spool limit the new snapshot is refused
// visibly (failed status) while old batches are preserved; once the remote
// drains, enqueue succeeds again.
func TestQueueBoundedBacklog(t *testing.T) {
	release := make(chan struct{})
	fp := newFakePusher()
	fp.release = release
	spy := &statusSpy{}
	q := newTestQueue(t, fp.push, spy, func(c *Config) { c.SpoolMaxBatches = 2 })
	q.Start()

	if err := q.Enqueue([]byte("b1"), "o"); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue([]byte("b2"), "o"); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool { _, _, max := fp.snapshot(); return max == 1 }, "first send in flight")

	err := q.Enqueue([]byte("b3"), "o")
	if !errors.Is(err, ErrSpoolFull) {
		t.Fatalf("third enqueue = %v, want ErrSpoolFull", err)
	}
	waitUntil(t, time.Second, func() bool { return spy.saw(CodeFailed) }, "failed state on full spool")

	// old batches are still intact and delivered in order after recovery
	close(release)
	waitUntil(t, 2*time.Second, func() bool { return q.Pending() == 0 }, "drain")

	sent, _, _ := fp.snapshot()
	if !slices.Equal(sent, []uint64{1, 2}) {
		t.Fatalf("sent = %v, want [1 2]", sent)
	}
	if err := q.Enqueue([]byte("b4"), "o"); err != nil {
		t.Fatalf("enqueue after drain: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool {
		snap := q.Snapshot()
		return snap.Acked == 3
	}, "new batch acked")
}

// A slow/blocked remote must not block enqueue: the collector path returns
// immediately and an unrelated exporter drains independently.
func TestQueueSlowRemoteDoesNotBlockEnqueue(t *testing.T) {
	release := make(chan struct{})
	slow := newFakePusher()
	slow.release = release
	spySlow := &statusSpy{}
	slowQ := newTestQueue(t, slow.push, spySlow, func(c *Config) { c.Name = "slow" })
	slowQ.Start()

	start := time.Now()
	if err := slowQ.Enqueue([]byte("slow-batch"), "o"); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("enqueue blocked for %v behind slow remote", d)
	}

	fast := newFakePusher()
	spyFast := &statusSpy{}
	fastQ := newTestQueue(t, fast.push, spyFast, func(c *Config) { c.Name = "fast" })
	fastQ.Start()
	if err := fastQ.Enqueue([]byte("fast-batch"), "o"); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, time.Second, func() bool { return fastQ.Pending() == 0 }, "fast queue drain")

	if slowQ.Pending() != 1 {
		t.Fatalf("slow queue should still hold its batch")
	}
	close(release)
	waitUntil(t, time.Second, func() bool { return slowQ.Pending() == 0 }, "slow queue drain")
}

// Exactly one batch is ever in flight per queue and a batch is never sent
// concurrently with itself.
func TestQueueSingleFlight(t *testing.T) {
	release := make(chan struct{})
	fp := newFakePusher()
	fp.release = release
	spy := &statusSpy{}
	q := newTestQueue(t, fp.push, spy, func(c *Config) { c.SpoolMaxBatches = 100 })
	q.Start()

	for i := 0; i < 10; i++ {
		if err := q.Enqueue([]byte("x"), "o"); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(50 * time.Millisecond) // let the first send park
	_, attempts, maxFlight := fp.snapshot()
	if maxFlight > 1 {
		t.Fatalf("concurrent sends detected, max in-flight = %d", maxFlight)
	}
	if len(attempts) > 1 {
		t.Fatalf("second batch started while head in flight: %v", attempts)
	}

	close(release)
	waitUntil(t, 2*time.Second, func() bool { return q.Pending() == 0 }, "drain")
	_, attempts, maxFlight = fp.snapshot()
	if maxFlight != 1 {
		t.Fatalf("max in-flight = %d, want 1", maxFlight)
	}
	for id := uint64(1); id <= 10; id++ {
		if attempts[id] != 1 {
			t.Fatalf("batch %d attempts = %d, want exactly 1", id, attempts[id])
		}
	}
}

// New batches are held only on disk once the in-memory window is full and
// still arrive intact at the remote.
func TestQueueMemoryWindowOverflow(t *testing.T) {
	release := make(chan struct{})
	fp := newFakePusher()
	fp.release = release
	spy := &statusSpy{}
	q := newTestQueue(t, fp.push, spy, func(c *Config) {
		c.QueueMax = 2
		c.SpoolMaxBatches = 100
	})
	q.Start()

	for i := 0; i < 10; i++ {
		if err := q.Enqueue([]byte(fmt.Sprintf("payload-%d", i)), "o"); err != nil {
			t.Fatal(err)
		}
	}
	close(release)
	waitUntil(t, 2*time.Second, func() bool { return q.Pending() == 0 }, "drain")

	for id := uint64(1); id <= 10; id++ {
		if fp.payloads[id] != fmt.Sprintf("payload-%d", id-1) {
			t.Fatalf("batch %d payload = %q", id, fp.payloads[id])
		}
	}
}
