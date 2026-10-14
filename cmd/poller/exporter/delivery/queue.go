/*
Copyright NetApp Inc, 2021 All rights reserved
*/

package delivery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Exporter status codes. They intentionally match the codes declared by the
// exporter package (0=up, 2=failed) and extend them with delivery-specific
// states (3=backlogged, 4=retrying, 5=rejected).
const (
	CodeUp         uint8 = 0
	CodeFailed     uint8 = 2
	CodeBacklogged uint8 = 3
	CodeRetrying   uint8 = 4
	CodeRejected   uint8 = 5
)

const (
	defaultQueueMax        = 256
	defaultSpoolMaxBatches = 10000
	defaultRetryMin        = 1 * time.Second
	defaultRetryMax        = 5 * time.Minute
)

// ErrSpoolFull is returned by Enqueue when the configured hard limit of
// unacknowledged batches is reached. Older batches are preserved; the new
// snapshot is refused (and the export error is logged by the collector).
var ErrSpoolFull = errors.New("delivery backlog limit reached, refusing new batch")

// PushFunc delivers one batch payload. Implementations return nil on
// acknowledgement or an error classified with ErrRetryable/ErrRejected,
// typically via delivery.Do.
type PushFunc func(ctx context.Context, batchID uint64, payload []byte) error

// Config configures a Queue.
type Config struct {
	Poller string
	Class  string
	Name   string
	Logger *slog.Logger
	// Dir is the per-exporter spool directory. Use SpoolDir to compute it.
	Dir string
	// QueueMax bounds payloads kept in memory. Zero selects the default.
	QueueMax int
	// SpoolMaxBatches bounds pending batches on disk. Zero selects default.
	SpoolMaxBatches int
	// RetryMin/RetryMax bound the deterministic exponential backoff.
	RetryMin time.Duration
	RetryMax time.Duration
	// Push performs one HTTP attempt.
	Push PushFunc
	// SetStatus publishes exporter status (code, reason) to the poller.
	SetStatus func(code uint8, reason string)
}

// State of the delivery pipeline.
type State uint8

const (
	StateUp State = iota
	StateBacklogged
	StateRetrying
	StateRejected
	StateFailed
)

func (s State) String() string {
	switch s {
	case StateUp:
		return "up"
	case StateBacklogged:
		return "backlogged"
	case StateRetrying:
		return "retrying"
	case StateRejected:
		return "rejected"
	default:
		return "failed"
	}
}

// Code maps a delivery state to the exporter status code.
func (s State) Code() uint8 {
	switch s {
	case StateUp:
		return CodeUp
	case StateBacklogged:
		return CodeBacklogged
	case StateRetrying:
		return CodeRetrying
	case StateRejected:
		return CodeRejected
	default:
		return CodeFailed
	}
}

// Batch is one rendered, spooled payload awaiting acknowledgement.
type Batch struct {
	id        uint64
	payload   []byte // nil when held only on disk
	source    string
	attempts  int
	nextTry   time.Time
	createdAt time.Time
}

// Stats is a point-in-time snapshot of the pipeline counters.
type Stats struct {
	Pending  int
	InMemory int
	Enqueued uint64
	Acked    uint64
	Retries  uint64
	Rejected uint64
}

// Queue is a per-exporter FIFO of durable batches with exactly one sender
// goroutine. A batch is removed only after acknowledgement. One Queue never
// shares state with other exporters: a slow or down remote cannot block
// another exporter, a collector or the poller schedule.
type Queue struct {
	cfg   Config
	spool *Spool

	mu       sync.Mutex
	pending  []*Batch
	inMemory int
	seq      uint64

	state        State
	reason       string
	lastRejected bool
	refusedFull  bool
	fatal        error

	enqueued uint64
	acked    uint64
	retries  uint64
	rejected uint64

	kick   chan struct{}
	stop   chan struct{}
	done   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
}

// SpoolDir returns the per-exporter spool directory:
// <base>/<poller>/<safe-exporter-name>.
func SpoolDir(base, poller, name string) string {
	return filepath.Join(base, safePathElem(poller), safePathElem(name))
}

var unsafeNameReplacer = strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")

func safePathElem(s string) string {
	s = unsafeNameReplacer.Replace(s)
	s = strings.Trim(s, ".")
	if s == "" {
		return "unnamed"
	}
	return s
}

// New opens (or creates) the delivery queue and recovers batches left
// unacknowledged by a previous process.
func New(cfg Config) (*Queue, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.QueueMax <= 0 {
		cfg.QueueMax = defaultQueueMax
	}
	if cfg.SpoolMaxBatches <= 0 {
		cfg.SpoolMaxBatches = defaultSpoolMaxBatches
	}
	if cfg.RetryMin <= 0 {
		cfg.RetryMin = defaultRetryMin
	}
	if cfg.RetryMax <= 0 || cfg.RetryMax < cfg.RetryMin {
		cfg.RetryMax = defaultRetryMax
	}
	if cfg.Push == nil {
		return nil, errors.New("delivery: missing Push function")
	}
	if cfg.SetStatus == nil {
		cfg.SetStatus = func(uint8, string) {}
	}
	if cfg.Dir == "" {
		return nil, errors.New("delivery: missing spool directory")
	}

	spool, recovered, err := NewSpool(cfg.Dir, cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("open spool %s: %w", cfg.Dir, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		cfg:    cfg,
		spool:  spool,
		kick:   make(chan struct{}, 1),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}

	for _, id := range recovered {
		q.pending = append(q.pending, &Batch{id: id, createdAt: time.Now()})
		if id > q.seq {
			q.seq = id
		}
	}
	q.refreshLocked()
	return q, nil
}

// Start launches the single sender goroutine.
func (q *Queue) Start() {
	if len(q.pending) > 0 {
		q.cfg.Logger.Info(
			"recovered unacknowledged batches, will redeliver in order",
			slog.Int("pending", len(q.pending)),
			slog.String("dir", q.cfg.Dir),
		)
		q.publish()
	}
	go q.run()
}

// Close stops the sender. Spooled batches stay on disk and are recovered on
// the next start.
func (q *Queue) Close() {
	select {
	case <-q.stop:
		return
	default:
		close(q.stop)
		q.cancel()
	}
	<-q.done
}

// Enqueue durably appends a batch to the FIFO. It performs local file IO
// only and therefore returns independently of the remote's health. When the
// spool is full or broken the error is returned and the export call fails
// visibly instead of silently dropping the snapshot.
func (q *Queue) Enqueue(payload []byte, source string) error {
	q.mu.Lock()
	if q.fatal != nil {
		err := q.fatal
		q.mu.Unlock()
		return err
	}
	if len(q.pending) >= q.cfg.SpoolMaxBatches {
		q.refusedFull = true
		q.refreshLocked()
		reason := q.reason
		q.mu.Unlock()
		q.cfg.SetStatus(CodeFailed, reason)
		q.cfg.Logger.Error(
			"backlog limit reached, snapshot not spooled",
			slog.Int("limit", q.cfg.SpoolMaxBatches),
			slog.String("source", source),
		)
		return ErrSpoolFull
	}
	id := q.seq + 1
	q.seq = id
	retain := q.inMemory < q.cfg.QueueMax
	q.mu.Unlock()

	path, err := q.spool.Write(id, payload)
	if err != nil {
		q.markFatal(fmt.Errorf("write spool: %w", err))
		return err
	}

	q.mu.Lock()
	b := &Batch{id: id, source: source, createdAt: time.Now()}
	if retain {
		b.payload = payload
		q.inMemory++
	}
	q.insertLocked(b)
	q.enqueued++
	q.refreshLocked()
	state, reason := q.state, q.reason
	q.mu.Unlock()

	q.cfg.Logger.Debug(
		"batch spooled",
		slog.Uint64("batch", id),
		slog.String("source", source),
		slog.Int("bytes", len(payload)),
		slog.String("file", path),
	)
	if state != StateUp {
		q.cfg.SetStatus(state.Code(), reason)
	}
	q.signal()
	return nil
}

// Pending returns the number of unacknowledged batches.
func (q *Queue) Pending() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// Snapshot returns counters for metadata exposition.
func (q *Queue) Snapshot() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return Stats{
		Pending:  len(q.pending),
		InMemory: q.inMemory,
		Enqueued: q.enqueued,
		Acked:    q.acked,
		Retries:  q.retries,
		Rejected: q.rejected,
	}
}

func (q *Queue) run() {
	defer close(q.done)

	for {
		q.mu.Lock()
		var wait time.Duration
		var head *Batch
		if q.fatal == nil && len(q.pending) > 0 {
			head = q.pending[0]
			if !head.nextTry.IsZero() {
				if d := time.Until(head.nextTry); d > 0 {
					wait = d
				}
			}
		}
		q.mu.Unlock()

		if head == nil {
			// Queue is empty or locally broken: sleep until a new batch is
			// spooled or shutdown. Fatal state is surfaced by Enqueue.
			select {
			case <-q.stop:
				return
			case <-q.kick:
				continue
			}
		}

		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-q.stop:
				timer.Stop()
				return
			case <-q.kick:
				timer.Stop()
				continue
			case <-timer.C:
			}
		}

		if !q.deliver(head) {
			return
		}
	}
}

// deliver performs one attempt for head and updates state. Returns false if
// the sender should stop.
func (q *Queue) deliver(head *Batch) bool {
	q.mu.Lock()
	payload := head.payload
	q.mu.Unlock()

	if payload == nil {
		// Batch was recovered from disk or evicted from the memory window.
		loaded, err := q.spool.Read(head.id)
		if err != nil {
			q.markFatal(fmt.Errorf("read spool batch %d: %w", head.id, err))
			return true
		}
		payload = loaded
	}

	err := q.cfg.Push(q.ctx, head.id, payload)

	switch {
	case err == nil:
		q.handleAck(head)
	case errors.Is(err, ErrRejected):
		q.handleReject(head, err)
	default:
		q.handleRetry(head, err)
	}

	select {
	case <-q.stop:
		return false
	default:
		return true
	}
}

func (q *Queue) handleAck(head *Batch) {
	if err := q.spool.Ack(head.id); err != nil {
		q.cfg.Logger.Warn("acknowledged but unable to remove spool file", slog.Uint64("batch", head.id), slog.Any("err", err))
	}

	q.mu.Lock()
	q.pending = q.pending[1:]
	if head.payload != nil {
		q.inMemory--
	}
	q.acked++
	q.lastRejected = false
	q.refusedFull = false
	q.refreshLocked()
	state, reason, depth := q.state, q.reason, len(q.pending)
	q.mu.Unlock()

	q.cfg.Logger.Debug("batch acknowledged", slog.Uint64("batch", head.id), slog.Int("pending", depth))
	q.cfg.SetStatus(state.Code(), reason)
	q.signal() // next batch may already be due
}

func (q *Queue) handleRetry(head *Batch, sendErr error) {
	head.attempts++
	wait := backoff(q.cfg.RetryMin, q.cfg.RetryMax, head.attempts)
	if se, ok := errRetryAfter(sendErr); ok && se > wait {
		wait = se
		if q.cfg.RetryMax > 0 && wait > q.cfg.RetryMax {
			wait = q.cfg.RetryMax
		}
	}
	head.nextTry = time.Now().Add(wait)

	q.mu.Lock()
	q.retries++
	q.lastRejected = false
	q.refreshLocked()
	state, reason := q.state, q.reason
	q.mu.Unlock()

	q.cfg.Logger.Warn(
		"batch not accepted, will retry",
		slog.Uint64("batch", head.id),
		slog.String("source", head.source),
		slog.Int("attempt", head.attempts),
		slog.Duration("retryIn", wait),
		slog.String("err", truncate(sendErr.Error(), 300)),
	)
	q.cfg.SetStatus(state.Code(), reason)
}

func (q *Queue) handleReject(head *Batch, sendErr error) {
	// Identical bytes cannot succeed on retry: quarantine for inspection and
	// continue with the next batch instead of looping forever.
	q.spool.Reject(head.id)

	q.mu.Lock()
	q.pending = q.pending[1:]
	if head.payload != nil {
		q.inMemory--
	}
	q.rejected++
	q.lastRejected = true
	q.refreshLocked()
	state, reason, depth := q.state, q.reason, len(q.pending)
	q.mu.Unlock()

	q.cfg.Logger.Error(
		"batch permanently rejected by remote, quarantined",
		slog.Uint64("batch", head.id),
		slog.String("source", head.source),
		slog.Int("pending", depth),
		slog.String("err", truncate(sendErr.Error(), 500)),
	)
	q.cfg.SetStatus(state.Code(), reason)
	q.signal()
}

func (q *Queue) markFatal(err error) {
	q.mu.Lock()
	q.fatal = err
	q.refreshLocked()
	state, reason := q.state, q.reason
	q.mu.Unlock()
	q.cfg.Logger.Error("delivery failed locally", slog.String("state", state.String()), slog.Any("err", err))
	q.cfg.SetStatus(state.Code(), reason)
}

// refreshLocked recomputes state/reason. Callers hold q.mu.
func (q *Queue) refreshLocked() {
	switch {
	case q.fatal != nil:
		q.state = StateFailed
		q.reason = "delivery failure: " + truncate(q.fatal.Error(), 200)
	case q.refusedFull:
		q.state = StateFailed
		q.reason = fmt.Sprintf("backlog limit %d reached, snapshots refused until backlog drains", q.cfg.SpoolMaxBatches)
	case len(q.pending) == 0:
		if q.lastRejected {
			q.state = StateRejected
			q.reason = fmt.Sprintf("remote rejected batch(es), %d rejected since start; waiting for next attempt", q.rejected)
		} else {
			q.state = StateUp
			q.reason = "running"
		}
	default:
		head := q.pending[0]
		switch {
		case head.attempts > 0:
			q.state = StateRetrying
			q.reason = fmt.Sprintf("retrying batch %d (attempt %d, %d pending)", head.id, head.attempts, len(q.pending))
		case q.lastRejected:
			q.state = StateRejected
			q.reason = fmt.Sprintf("remote rejected batch(es), %d rejected since start; %d pending", q.rejected, len(q.pending))
		default:
			q.state = StateBacklogged
			q.reason = fmt.Sprintf("%d batch(es) pending delivery", len(q.pending))
		}
	}
}

func (q *Queue) publish() {
	q.mu.Lock()
	state, reason := q.state, q.reason
	q.mu.Unlock()
	q.cfg.SetStatus(state.Code(), reason)
}

func (q *Queue) insertLocked(b *Batch) {
	// Fast path: callers are serialized, new ids are the largest.
	if len(q.pending) == 0 || q.pending[len(q.pending)-1].id < b.id {
		q.pending = append(q.pending, b)
		return
	}
	for i, x := range q.pending {
		if x.id > b.id {
			q.pending = append(q.pending[:i], append([]*Batch{b}, q.pending[i:]...)...)
			return
		}
	}
	q.pending = append(q.pending, b)
}

func (q *Queue) signal() {
	select {
	case q.kick <- struct{}{}:
	default:
	}
}

func backoff(min, max time.Duration, attempt int) time.Duration {
	d := min
	for i := 1; i < attempt; i++ {
		d *= 2
		if d <= 0 || d >= max {
			return max
		}
	}
	if d > max {
		return max
	}
	return d
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// errRetryAfter extracts the server-demanded Retry-After delay, if any.
func errRetryAfter(err error) (time.Duration, bool) {
	var se *Error
	if errors.As(err, &se) {
		if d := se.RetryAfter(); d > 0 {
			return d, true
		}
	}
	return 0, false
}
