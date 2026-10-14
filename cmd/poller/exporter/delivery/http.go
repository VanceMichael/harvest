/*
Copyright NetApp Inc, 2021 All rights reserved

HTTP delivery helpers shared by push exporters. A send attempt has exactly
three outcomes:

  - acknowledged (nil): the remote accepted the batch, it can be deleted;
  - retryable (ErrRetryable): transport error, timeout, 408/429 or 5xx.
    The batch stays at the head of the queue and is retried with backoff;
  - rejected (ErrRejected): any other 4xx. Retrying identical bytes cannot
    help, the batch is quarantined and the exporter enters an observable
    rejected state.
*/
package delivery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// BatchHeader carries the unique batch id. The write endpoints of
	// InfluxDB/VictoriaMetrics ignore unknown headers; it correlates server
	// logs with spooled batches and prevents ambiguous retries.
	BatchHeader = "X-Harvest-Batch-Id"

	maxErrorBody = 4096
)

var (
	// ErrRetryable is matched by errors.Is for failures the sender retries.
	ErrRetryable = errors.New("delivery failed, retryable")
	// ErrRejected is matched by errors.Is for failures the sender does
	// not retry: the remote explicitly refused the batch (non-retryable 4xx).
	ErrRejected = errors.New("delivery permanently rejected by remote")
)

// Error is the classified result of one HTTP send attempt.
type Error struct {
	kind       error
	statusCode int
	retryAfter time.Duration
	inner      error
}

func (e *Error) Error() string {
	var b strings.Builder
	if e.statusCode != 0 {
		fmt.Fprintf(&b, "HTTP %d", e.statusCode)
	}
	if e.inner != nil {
		if b.Len() > 0 {
			b.WriteString(": ")
		}
		b.WriteString(e.inner.Error())
	}
	return b.String()
}

func (e *Error) Unwrap() error { return e.inner }

// Is maps the concrete Error to the ErrRetryable/ErrRejected sentinels so
// errors.Is works without unwrapping the transport cause.
func (e *Error) Is(target error) bool { return target == e.kind }

// RetryAfter reports the server-demanded wait (0 if absent).
func (e *Error) RetryAfter() time.Duration { return e.retryAfter }

// StatusCode reports the HTTP status code (0 for transport errors).
func (e *Error) StatusCode() int { return e.statusCode }

// RetryableError wraps a local error (request construction, ...) as
// retryable.
func RetryableError(err error) error {
	return &Error{kind: ErrRetryable, inner: err}
}

// Do executes req with client and classifies the outcome. The caller owns
// building the request (URL, auth headers, body).
func Do(ctx context.Context, client *http.Client, req *http.Request) error {
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		// Every transport-level failure (connection refused/reset, DNS,
		// deadline, client timeout) is retryable: the batch was never
		// provably accepted.
		return &Error{kind: ErrRetryable, inner: err}
	}
	//goland:noinspection GoUnhandledErrorResult
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusRequestTimeout ||
		resp.StatusCode == http.StatusTooManyRequests ||
		resp.StatusCode >= 500:
		return &Error{
			kind:       ErrRetryable,
			statusCode: resp.StatusCode,
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
			inner:      errors.New(strings.TrimSpace(string(body))),
		}
	case resp.StatusCode >= 400:
		return &Error{
			kind:       ErrRejected,
			statusCode: resp.StatusCode,
			inner:      errors.New(strings.TrimSpace(string(body))),
		}
	default:
		// 1xx/3xx are not produced by the write endpoints; treat them as
		// transient rather than dropping the batch.
		return &Error{kind: ErrRetryable, statusCode: resp.StatusCode, inner: errors.New("unexpected status")}
	}
}

// parseRetryAfter parses an RFC 9110 Retry-After value, either delta-seconds
// or an HTTP date. A zero/invalid value yields 0.
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}
