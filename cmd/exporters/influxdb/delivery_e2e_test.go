/*
Copyright NetApp Inc, 2021 All rights reserved

End-to-end tests for the durable delivery pipeline of the InfluxDB
exporter: a real HTTP server (httptest), real spool on disk and the
exporter's background sender goroutine.
*/
package influxdb

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/netapp/harvest/v2/assert"
	"github.com/netapp/harvest/v2/cmd/poller/exporter"
	"github.com/netapp/harvest/v2/cmd/poller/options"
	"github.com/netapp/harvest/v2/pkg/conf"
	"github.com/netapp/harvest/v2/pkg/matrix"
)

type scriptedServer struct {
	mu     sync.Mutex
	bodies []string
	status atomic.Int32
}

func (s *scriptedServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.bodies = append(s.bodies, string(body))
		s.mu.Unlock()
		code := s.status.Load()
		if code == http.StatusNoContent {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(int(code))
		_, _ = w.Write([]byte("scripted failure"))
	})
}

func (s *scriptedServer) bodiesContain(substr string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.bodies {
		if strings.Contains(b, substr) {
			return true
		}
	}
	return false
}

func setupPushInfluxDB(t *testing.T, serverURL, spoolPath string) *InfluxDB {
	t.Helper()
	opts := options.New()
	opts.IsTest = false
	opts.Poller = "e2e-poller"

	url := serverURL
	token := "test-token"
	params := conf.Exporter{
		URL:   &url,
		Token: &token,
		Delivery: &conf.DeliveryConfig{
			SpoolPath:        spoolPath,
			RetryMinInterval: "10ms",
			RetryMaxInterval: "50ms",
		},
	}

	e := &InfluxDB{AbstractExporter: exporter.New("InfluxDB", "e2e-influx", opts, params, nil)}
	assert.Nil(t, e.Init())
	t.Cleanup(func() { e.queue.Close() })
	return e
}

func matrixForObject(object string, value int64) *matrix.Matrix {
	data := matrix.New("test_exporter", object, object)
	m, _ := data.NewMetricInt64("test_metric")
	i, _ := data.NewInstance("test_instance")
	i.SetLabel("test_label", "test_label_value")
	m.SetValueInt64(i, value)
	return data
}

func waitStatus(t *testing.T, e *InfluxDB, code uint8) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c, _, _ := e.GetStatus(); c == code {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	c, s, m := e.GetStatus()
	t.Fatalf("wait status %d timed out, got %d %s %s", code, c, s, m)
}

// Happy path plus outage/recovery: Export never blocks, the failed round is
// spooled, retried through a 500 outage and delivered after recovery;
// status transitions are visible.
func TestInfluxDeliveryRetryAndRecovery(t *testing.T) {
	srv := &scriptedServer{}
	srv.status.Store(http.StatusNoContent)
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	slog.SetLogLoggerLevel(slog.LevelError)
	e := setupPushInfluxDB(t, ts.URL, filepath.Join(t.TempDir(), "spool"))

	// 1) happy path: data and exporter metadata are accepted
	_, err := e.Export(matrixForObject("e2e_happy", 1))
	assert.Nil(t, err)
	waitStatus(t, e, 0)
	if !srv.bodiesContain("e2e_happy") {
		t.Fatalf("server did not receive happy batch: %v", srv.bodies)
	}

	// 2) remote starts failing: Export itself must not error or block,
	// and the exporter must visibly enter the retrying state.
	srv.status.Store(http.StatusServiceUnavailable)
	_, err = e.Export(matrixForObject("e2e_backlog", 2))
	assert.Nil(t, err)
	waitStatus(t, e, 4)
	if c, s, _ := e.GetStatus(); c != 4 || s != "retrying" {
		t.Fatalf("want retrying, got %d %s", c, s)
	}

	// 3) remote recovers: old batch is delivered first, status back to up
	srv.status.Store(http.StatusNoContent)
	waitStatus(t, e, 0)
	if !srv.bodiesContain("e2e_backlog") {
		t.Fatalf("backlogged batch lost, bodies: %v", srv.bodies)
	}

	// 4) permanent rejection is observable and does not stall the queue
	srv.status.Store(http.StatusBadRequest)
	_, err = e.Export(matrixForObject("e2e_bad", 3))
	assert.Nil(t, err)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if e.queue.Snapshot().Rejected >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if e.queue.Snapshot().Rejected < 1 {
		t.Fatalf("batch was not marked rejected")
	}
	if c, _, _ := e.GetStatus(); c != 5 {
		t.Fatalf("want rejected(5), got %d", c)
	}

	// 5) after the remote answers again, new batches flow normally
	srv.status.Store(http.StatusNoContent)
	_, err = e.Export(matrixForObject("e2e_after", 4))
	assert.Nil(t, err)
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if srv.bodiesContain("e2e_after") {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("post-rejection batch not delivered: %v", srv.bodies)
}
