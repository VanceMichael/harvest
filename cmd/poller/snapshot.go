/*
Copyright NetApp Inc, 2026 All rights reserved

This file implements the poller's runtime-state snapshots. Each scheduled
status report builds one immutable generation: fresh, independent copies of
the metadata matrices plus the per-collector metadata snapshots. The
generation is published with a single atomic pointer swap and is then handed
to exporters, logging and AutoSupport; producers building the next generation
never mutate a published one.
*/

package main

import (
	"log/slog"
	"runtime"

	"github.com/netapp/harvest/v2/cmd/poller/collector"
	"github.com/netapp/harvest/v2/pkg/matrix"
	"github.com/netapp/harvest/v2/pkg/slogx"
)

// snapshot is one immutable generation of the poller's runtime state.
// All three matrices and every collector metadata matrix are independently
// owned by the generation and are never modified after publishing.
type snapshot struct {
	seq uint64

	// components is metadata_component: status/count of each collector and exporter
	components *matrix.Matrix
	// target is metadata_target: target reachability, ping, goroutine count
	target *matrix.Matrix
	// status is poller_status: poller reachability and memory metrics
	status *matrix.Matrix

	// collectorMD maps "Name.Object" to the collector's own published metadata snapshot
	collectorMD map[string]*matrix.Matrix
}

// asupInput packages the parts of the generation consumed when building
// AutoSupport. AutoSupport gets exactly the same generation data as the
// regular exporters, even when the two overlap.
func (s *snapshot) asupInput() *collector.Snapshot {
	return &collector.Snapshot{
		Target:            s.target,
		CollectorMetadata: s.collectorMD,
	}
}

// newGenerationMatrix clones the template (metrics, instances and static labels)
// and resets the metric values, yielding a matrix the generation exclusively owns
func newGenerationMatrix(template *matrix.Matrix) *matrix.Matrix {
	m := template.CloneForCollection()
	m.Reset()
	return m
}

// buildSnapshot constructs the next generation from independent matrix copies and
// atomically reads every producer state. It performs no I/O against exporters, so a
// consumer failure cannot affect the build. Returns the generation and the counts of
// collectors/exporters that are currently up.
func (p *Poller) buildSnapshot(seq uint64) (*snapshot, int, int) {

	components := newGenerationMatrix(p.metadata)
	target := newGenerationMatrix(p.metadataTarget)
	status := newGenerationMatrix(p.status)

	targetHost := target.MustGetInstance("host")
	statusHost := status.MustGetInstance("host")

	// ping target system
	if ping, ok := p.ping(); ok {
		target.MustSetValueUint8("status", targetHost, 0)
		target.MustSetValueFloat64("ping", targetHost, float64(ping))
		status.MustSetValueUint8("status", statusHost, 1)
	} else {
		target.MustSetValueUint8("status", targetHost, 1)
		status.MustSetValueUint8("status", statusHost, 0)
	}

	p.addMemoryMetadata(status, statusHost)

	// add number of goroutines to target metadata
	target.MustSetValueInt64("goroutines", targetHost, int64(runtime.NumGoroutine()))

	upc := 0 // up collectors
	upe := 0 // up exporters

	collectorMD := make(map[string]*matrix.Matrix, len(p.collectors))

	// update status of collectors
	for _, c := range p.collectors {
		code, _, msg := c.GetStatus()

		if code == 0 {
			upc++
		}

		key := c.GetName() + "." + c.GetObject()

		// cumulative producer total minus the previous generation baseline:
		// each produced data point is counted by exactly one generation
		total := c.GetCollectedTotal()
		count := total - p.lastCollected[key]
		p.lastCollected[key] = total

		collectorInst := components.MustGetInstance(key)
		components.MustSetValueUint64("count", collectorInst, count)
		components.MustSetValueUint8("status", collectorInst, code)

		if msg != "" {
			collectorInst.SetLabel("reason", p.truncateReason(msg))
		}

		collectorMD[key] = c.GetMetadataSnapshot()
	}

	// add remote version and name to poller status
	status.GetInstance("remote").SetLabel("version", p.remote.Version)
	status.GetInstance("remote").SetLabel("name", p.remote.Name)

	// update status of exporters
	for _, ee := range p.exporters {
		code, statusName, msg := ee.GetStatus()
		logger.Debug(
			"exporter status",
			slog.String("name", ee.GetName()),
			slog.Int("code", int(code)),
			slog.String("status", statusName),
			slog.String("msg", msg),
		)

		if code == 0 {
			upe++
		}

		key := ee.GetClass() + "." + ee.GetName()

		total := ee.GetExportedTotal()
		count := total - p.lastExported[key]
		p.lastExported[key] = total

		exporterInst := components.MustGetInstance(key)
		components.MustSetValueUint64("count", exporterInst, count)
		components.MustSetValueUint8("status", exporterInst, code)

		if msg != "" {
			exporterInst.SetLabel("reason", p.truncateReason(msg))
		}
	}

	return &snapshot{
		seq:         seq,
		components:  components,
		target:      target,
		status:      status,
		collectorMD: collectorMD,
	}, upc, upe
}

// deliverToExporters hands each exporter its own independent copy of the generation.
// A failed export is logged and skipped without affecting the published generation,
// the other exporters or AutoSupport. A subsequent successful generation resumes
// normal delivery; no partial state is reused.
func (p *Poller) deliverToExporters(s *snapshot) {
	for _, ee := range p.exporters {
		if _, err := ee.Export(s.components.Clone()); err != nil {
			logger.Error("export component metadata", slogx.Err(err))
		}
		if _, err := ee.Export(s.target.Clone()); err != nil {
			logger.Error("export target metadata", slogx.Err(err))
		}
		if _, err := ee.Export(s.status.Clone()); err != nil {
			logger.Error("export poller status", slogx.Err(err))
		}
	}
}
