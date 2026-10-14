package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/netapp/harvest/v2/assert"
	collectorPkg "github.com/netapp/harvest/v2/cmd/poller/collector"
	exporterPkg "github.com/netapp/harvest/v2/cmd/poller/exporter"
	"github.com/netapp/harvest/v2/cmd/poller/options"
	"github.com/netapp/harvest/v2/pkg/conf"
	"github.com/netapp/harvest/v2/pkg/matrix"
	"github.com/netapp/harvest/v2/pkg/tree/node"
)

// fakeExporter embeds AbstractExporter for the status/count machinery and
// only overrides Init and Export. Optional hooks simulate failures, input
// mutation and per-export verification.
type fakeExporter struct {
	*exporterPkg.AbstractExporter
	mu      sync.Mutex
	objects []string
	fail    bool
	mutate  bool
	check   func(*matrix.Matrix)
}

func (f *fakeExporter) Init() error {
	return f.InitAbc()
}

func (f *fakeExporter) Export(data *matrix.Matrix) (exporterPkg.Stats, error) {
	f.Lock()
	defer f.Unlock()

	f.mu.Lock()
	f.objects = append(f.objects, data.Object)
	f.mu.Unlock()

	if f.check != nil {
		f.check(data)
	}
	if f.mutate {
		for _, inst := range data.GetInstances() {
			inst.SetLabel("tampered", "yes")
			inst.SetLabel("reason", "tampered")
		}
	}
	if f.fail {
		return exporterPkg.Stats{}, errors.New("exporter boom")
	}
	return exporterPkg.Stats{}, nil
}

func (f *fakeExporter) exportedObjects() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.objects
}

// newSnapshotTestPoller builds a Poller with nCollectors collectors and
// nExporters exporters plus matching metadata template instances
func newSnapshotTestPoller(t *testing.T, nCollectors, nExporters int) *Poller {
	t.Helper()
	p := &Poller{
		name:                 "test",
		target:               "localhost",
		options:              options.New(),
		params:               &conf.Poller{},
		lastCollected:        map[string]uint64{},
		lastExported:         map[string]uint64{},
		concurrentCollectors: &atomic.Int32{},
	}
	p.loadMetadata()

	for i := range nCollectors {
		// "Simple" collectors are not pingable, so buildSnapshot performs no network I/O
		name := "Simple"
		object := fmt.Sprintf("obj%d", i)
		tc := &testCollector{collectorPkg.New(name, object, p.options, node.NewS(""), nil, conf.Remote{})}
		p.collectors = append(p.collectors, tc)

		key := name + "." + object
		inst, err := p.metadata.NewInstance(key)
		assert.Nil(t, err)
		inst.SetLabel("type", "collector")
		inst.SetLabel("name", name)
		inst.SetLabel("target", object)
	}

	for i := range nExporters {
		name := fmt.Sprintf("exp%d", i)
		fe := &fakeExporter{
			AbstractExporter: exporterPkg.New("Fake", name, p.options, conf.Exporter{}, nil),
		}
		assert.Nil(t, fe.Init())
		p.exporters = append(p.exporters, fe)

		key := "Fake." + name
		inst, err := p.metadata.NewInstance(key)
		assert.Nil(t, err)
		inst.SetLabel("type", "exporter")
		inst.SetLabel("name", "Fake")
		inst.SetLabel("target", name)
	}

	return p
}

func collectorKey(c collectorPkg.Collector) string {
	return c.GetName() + "." + c.GetObject()
}

func exporterKey(e exporterPkg.Exporter) string {
	return e.GetClass() + "." + e.GetName()
}

func countValue(m *matrix.Matrix, key string) uint64 {
	v, _ := m.GetMetric("count").GetValueUint64(m.GetInstance(key))
	return v
}

// TestStatusCodeReasonAtomic: while status is updated concurrently, every reader
// must observe a code whose reason and status name match the same update.
func TestStatusCodeReasonAtomic(t *testing.T) {

	run := func(set func(uint8, string), get func() (uint8, string, string)) {
		// establish a known initial state before readers start
		set(0, "reason-0")
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range 3000 {
					code := uint8(i % 3)
					set(code, fmt.Sprintf("reason-%d", code))
				}
			}()
		}
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 3000 {
					code, name, reason := get()
					if want := fmt.Sprintf("reason-%d", code); reason != want {
						t.Errorf("code=%d reason=%q want %q", code, reason, want)
					}
					if want := collectorPkg.Status[code]; name != want {
						t.Errorf("code=%d statusName=%q want %q", code, name, want)
					}
				}
			}()
		}
		wg.Wait()
	}

	c := collectorPkg.New("Rest", "Volume", options.New(), node.NewS(""), nil, conf.Remote{})
	run(c.SetStatus, c.GetStatus)

	e := exporterPkg.New("Fake", "e", options.New(), conf.Exporter{}, nil)
	var names = [3]string{"up", "standby", "failed"}
	run(e.SetStatus, func() (uint8, string, string) {
		code, name, reason := e.GetStatus()
		if want := names[code]; name != want {
			t.Errorf("exporter code=%d statusName=%q want %q", code, name, want)
		}
		return code, name, reason
	})
}

// TestGenerationCountsExactlyOnce: counts accumulate cumulatively on producers,
// and each per-generation delta partitions the total without loss or duplication.
func TestGenerationCountsExactlyOnce(t *testing.T) {
	p := newSnapshotTestPoller(t, 3, 2)

	g1, upc, upe := p.buildSnapshot(1)
	p.published.Store(g1)
	assert.Equal(t, upc, 3)
	assert.Equal(t, upe, 2)

	for _, c := range p.collectors {
		assert.Equal(t, countValue(g1.components, collectorKey(c)), uint64(0))
	}
	for _, e := range p.exporters {
		assert.Equal(t, countValue(g1.components, exporterKey(e)), uint64(0))
	}

	// producers accumulate between generations
	for _, c := range p.collectors {
		c.AddCollectCount(10)
	}
	for _, e := range p.exporters {
		e.AddExportCount(7)
	}

	g2, _, _ := p.buildSnapshot(2)

	// first generation is immutable and independent of the next one
	for _, c := range p.collectors {
		key := collectorKey(c)
		assert.Equal(t, countValue(g1.components, key), uint64(0))
		assert.Equal(t, countValue(g2.components, key), uint64(10))
	}
	for _, e := range p.exporters {
		key := exporterKey(e)
		assert.Equal(t, countValue(g1.components, key), uint64(0))
		assert.Equal(t, countValue(g2.components, key), uint64(7))
	}

	// template is never reset/mutated by generation builds
	for _, c := range p.collectors {
		_, recorded := p.metadata.GetMetric("status").GetValueUint8(p.metadata.GetInstance(collectorKey(c)))
		assert.False(t, recorded)
	}

	gens := []*snapshot{g1, g2}
	for g := 3; g <= 6; g++ {
		for _, c := range p.collectors {
			c.AddCollectCount(uint64(g))
		}
		for _, e := range p.exporters {
			e.AddExportCount(uint64(g))
		}
		s, _, _ := p.buildSnapshot(uint64(g))
		gens = append(gens, s)
	}

	// sum of per-generation deltas equals the producer's cumulative total
	for _, c := range p.collectors {
		var sum uint64
		for _, g := range gens {
			sum += countValue(g.components, collectorKey(c))
		}
		assert.Equal(t, sum, c.GetCollectedTotal())
	}
	for _, e := range p.exporters {
		var sum uint64
		for _, g := range gens {
			sum += countValue(g.components, exporterKey(e))
		}
		assert.Equal(t, sum, e.GetExportedTotal())
	}
}

// TestConsumerIsolation: a failing exporter that mutates its input cannot affect
// another exporter's input or the published generation.
func TestConsumerIsolation(t *testing.T) {
	p := newSnapshotTestPoller(t, 2, 2)

	g, _, _ := p.buildSnapshot(1)
	p.published.Store(g)

	bad := p.exporters[0].(*fakeExporter)
	bad.fail = true
	bad.mutate = true

	var sawTampered atomic.Bool
	good := p.exporters[1].(*fakeExporter)
	good.check = func(m *matrix.Matrix) {
		for _, inst := range m.GetInstances() {
			if inst.GetLabel("tampered") == "yes" {
				sawTampered.Store(true)
			}
		}
	}

	p.deliverToExporters(g)

	assert.False(t, sawTampered.Load())
	for _, inst := range g.components.GetInstances() {
		assert.Equal(t, inst.GetLabel("tampered"), "")
	}
	for _, inst := range g.target.GetInstances() {
		assert.Equal(t, inst.GetLabel("tampered"), "")
	}
	for _, inst := range g.status.GetInstances() {
		assert.Equal(t, inst.GetLabel("tampered"), "")
	}
}

// TestDeliveryRecovery: an exporter failure does not prevent the next generation
// from being delivered fully to the other exporter; publishing is unaffected.
func TestDeliveryRecovery(t *testing.T) {
	p := newSnapshotTestPoller(t, 1, 2)

	g1, _, _ := p.buildSnapshot(1)
	p.published.Store(g1)

	bad := p.exporters[0].(*fakeExporter)
	bad.fail = true
	good := p.exporters[1].(*fakeExporter)

	p.deliverToExporters(g1)
	first := len(good.exportedObjects())
	assert.Equal(t, first, 3)

	g2, _, _ := p.buildSnapshot(2)
	p.published.Store(g2)
	p.deliverToExporters(g2)

	// the healthy exporter received all three matrices again despite the other failing
	assert.Equal(t, len(good.exportedObjects()), first+3)
	assert.True(t, p.published.Load() == g2)
}

// TestPublishedSnapshotImmutability: after newer generations are built and
// producers keep updating, the published generation stays byte-for-byte stable.
func TestPublishedSnapshotImmutability(t *testing.T) {
	p := newSnapshotTestPoller(t, 2, 1)

	g1, _, _ := p.buildSnapshot(1)
	p.published.Store(g1)
	g1Status, _ := g1.status.GetMetric("status").GetValueUint8(g1.status.GetInstance("host"))
	g1Ping, _ := g1.target.GetMetric("ping").GetValueFloat64(g1.target.GetInstance("host"))

	for _, c := range p.collectors {
		c.AddCollectCount(100)
	}
	for range 3 {
		g, _, _ := p.buildSnapshot(0)
		p.published.Store(g)
	}

	published := p.published.Load()
	_ = published
	// reload the first generation through its own pointer and check it's unchanged
	status, _ := g1.status.GetMetric("status").GetValueUint8(g1.status.GetInstance("host"))
	ping, _ := g1.target.GetMetric("ping").GetValueFloat64(g1.target.GetInstance("host"))
	assert.Equal(t, status, g1Status)
	assert.Equal(t, ping, g1Ping)
	for _, c := range p.collectors {
		assert.Equal(t, countValue(g1.components, collectorKey(c)), uint64(0))
	}
}
