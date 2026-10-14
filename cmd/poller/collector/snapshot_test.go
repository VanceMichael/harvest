/*
Copyright NetApp Inc, 2026 All rights reserved

White-box tests for the collector-side snapshot machinery:
 - metadata snapshots can be read concurrently with the next collection
 - only completed collection cycles publish; failed polls don't publish half-products,
   and the next successful poll restores a complete snapshot.
*/

package collector

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/netapp/harvest/v2/cmd/poller/options"
	"github.com/netapp/harvest/v2/pkg/conf"
	"github.com/netapp/harvest/v2/pkg/matrix"
	"github.com/netapp/harvest/v2/pkg/tree/node"
)

// TestMetadataSnapshotRace: one goroutine simulates the collector mutating its live
// metadata and publishing snapshots, concurrent readers consume the snapshots.
func TestMetadataSnapshotRace(t *testing.T) {
	c := New("Test", "x", options.New(), node.NewS(""), nil, conf.Remote{})

	md := matrix.New("Test", "metadata_collector", "x")
	_, _ = md.NewMetricInt64("poll_time")
	inst, _ := md.NewInstance("data")
	c.SetMetadata(md)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range int64(5000) {
			md.GetMetric("poll_time").SetValueInt64(inst, i)
			c.publishMetadata()
		}
	}()

	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 5000 {
				snap := c.GetMetadataSnapshot()
				_, _ = snap.GetMetric("poll_time").GetValueInt64(snap.GetInstance("data"))
			}
		}()
	}

	wg.Wait()
}

// scriptedCollector runs a deterministic poll sequence: every 5th poll fails,
// successful polls record a marker in the "metrics" metric. It stops the Start
// loop with a panic after seq 20.
type scriptedCollector struct {
	*AbstractCollector
	mu      sync.Mutex
	seq     int
	success []int64
}

func (s *scriptedCollector) Init(*AbstractCollector) error { return nil }

func (s *scriptedCollector) PollData() (map[string]*matrix.Matrix, error) {
	s.mu.Lock()
	v := s.seq
	s.seq++
	s.mu.Unlock()

	if v == 20 {
		panic("stop collector")
	}
	if v%5 == 0 {
		return nil, errors.New("boom poll error")
	}

	iv := int64(v)
	inst := s.Metadata.MustGetInstance("data")
	s.Metadata.MustSetValueUint64("metrics", inst, uint64(iv))
	s.mu.Lock()
	s.success = append(s.success, iv)
	s.mu.Unlock()

	return map[string]*matrix.Matrix{}, nil
}

// TestStartPublishesOnlyCompleteCycles: a failed poll must not publish a half-built
// snapshot (consumers keep the last complete one); on the next successful poll a full
// snapshot is published again. Status code and reason stay consistent throughout.
func TestStartPublishesOnlyCompleteCycles(t *testing.T) {

	params := node.NewS("")
	sch := params.NewChildS("schedule", "")
	sch.NewChildS("data", "2ms")

	ac := New("Test", "x", options.New(), params, nil, conf.Remote{})
	s := &scriptedCollector{AbstractCollector: ac}
	if err := Init(s); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// collect the set of successful markers
	successSet := func() map[int64]struct{} {
		s.mu.Lock()
		defer s.mu.Unlock()
		m := make(map[int64]struct{}, len(s.success))
		for _, v := range s.success {
			m[v] = struct{}{}
		}
		return m
	}

	stop := make(chan struct{})
	var readerWg sync.WaitGroup
	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				// every published marker must belong to a completed (successful) poll
				snap := s.GetMetadataSnapshot()
				data := snap.GetInstance("data")
				if marker, ok := snap.GetMetric("metrics").GetValueUint64(data); ok {
					if _, valid := successSet()[int64(marker)]; !valid {
						t.Errorf("published half-product marker=%d", marker)
					}
				}

				// code and reason must always correspond
				code, _, reason := s.GetStatus()
				switch code {
				case 0:
					if reason != "running" {
						t.Errorf("code=0 reason=%q", reason)
					}
				case 2:
					if reason != "boom poll error" {
						t.Errorf("code=2 reason=%q", reason)
					}
				}
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	s.Start(&wg, nil, &atomic.Int32{})
	wg.Wait()

	close(stop)
	readerWg.Wait()

	// expected successful polls: 1..19 except 5,10,15
	var want []int64
	for i := int64(1); i < 20; i++ {
		if i%5 != 0 {
			want = append(want, i)
		}
	}
	s.mu.Lock()
	got := slices.Clone(s.success)
	s.mu.Unlock()
	if !slices.Equal(got, want) {
		t.Errorf("successful polls got=%v want=%v", got, want)
	}

	// last poll (19) succeeded, so the final published snapshot carries its marker
	snap := s.GetMetadataSnapshot()
	marker, recorded := snap.GetMetric("metrics").GetValueUint64(snap.GetInstance("data"))
	if !recorded || marker != 19 {
		t.Errorf("final snapshot marker=%d recorded=%v, want 19", marker, recorded)
	}

	// and the collector recovered to up with the matching reason
	code, _, reason := s.GetStatus()
	if code != 0 || reason != "running" {
		t.Errorf("final status code=%d reason=%q, want 0/running", code, reason)
	}
}
