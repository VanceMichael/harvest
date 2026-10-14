/*
Copyright NetApp Inc, 2021 All rights reserved

Package exporter provides the Exporter interface and
type AbstractExporter that implements most basic
functions.
*/

package exporter

import (
	"github.com/netapp/harvest/v2/cmd/poller/options"
	"github.com/netapp/harvest/v2/pkg/conf"
	"github.com/netapp/harvest/v2/pkg/matrix"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
)

// Exporter defines the required attributes of an exporter
// All except, Export() are implemented by AbstractExporter
type Exporter interface {
	Init() error      // initialize exporter
	GetClass() string // the class of the exporter, e.g. Prometheus, InfluxDB
	// GetName is different from Class, since we can have multiple instances of the same Class
	GetName() string                      // the name of the exporter instance
	GetExportedTotal() uint64             // cumulative number of data points exported since start
	AddExportCount(uint64)                // add count to the exported total, called by the exporter itself
	GetStatus() (uint8, string, string)   // return current state of the exporter
	Export(*matrix.Matrix) (Stats, error) // render data in matrix to the desired format and emit
	// this is the only function that should be implemented by "real" exporters
}

// statusName defines the possible states of an exporter
var statusName = [3]string{
	"up",
	"standby",
	"failed",
}

// exporterStatus is the atomically published state of an exporter
type exporterStatus struct {
	code   uint8
	reason string
}

// Stats capture the number of instances and metrics exported
type Stats struct {
	InstancesExported uint64
	MetricsExported   uint64
	RenderedBytes     uint64
}

// AbstractExporter implements all methods of the Exporter interface, except Export()
// It defines attributes that will be "inherited" by child exporters
type AbstractExporter struct {
	Class       string
	Name        string
	Logger      *slog.Logger
	Options     *options.Options
	Params      conf.Exporter
	Metadata    *matrix.Matrix // metadata about the export
	*sync.Mutex                // mutex to block exporter during export

	// status is the atomically published state: code and reason are read/written as one
	// value, so a concurrent reader always sees a status code with its matching reason
	status atomic.Pointer[exporterStatus]

	// exportedTotal is the cumulative number of emitted data points since start.
	// Per-generation deltas are computed by the poller against the previous generation,
	// so publishing never mutates producer state and each count is attributed exactly once
	exportedTotal atomic.Uint64
}

// New creates an AbstractExporter instance with the given arguments:
// @c - exporter class
// @n - exporter name
// @o - poller options
// @p - exporter parameters
func New(c, n string, o *options.Options, p conf.Exporter, params *conf.Poller) *AbstractExporter {
	abc := AbstractExporter{
		Class:    c,
		Name:     n,
		Options:  o,
		Params:   p,
		Logger:   slog.Default().With(slog.String("exporter", n)),
		Mutex:    &sync.Mutex{},
		Metadata: matrix.New(n, "metadata_exporter", "metadata_exporter"),
	}
	abc.status.Store(&exporterStatus{})
	if params != nil {
		abc.Metadata.SetGlobalLabel("datacenter", params.Datacenter)
		labels := params.Labels
		if labels != nil {
			for _, labelPtr := range *labels {
				abc.Metadata.SetGlobalLabelsIfAbsent(labelPtr)
			}
		}
	}
	return &abc
}

// InitAbc initializes AbstractExporter
func (e *AbstractExporter) InitAbc() error {
	e.Metadata.SetGlobalLabel("hostname", e.Options.Hostname)
	e.Metadata.SetGlobalLabel("version", e.Options.Version)
	e.Metadata.SetGlobalLabel("poller", e.Options.Poller)
	e.Metadata.SetGlobalLabel("exporter", e.Class)
	e.Metadata.SetGlobalLabel("target", e.Name)

	if _, err := e.Metadata.NewMetricInt64("time"); err != nil {
		return err
	}
	if _, err := e.Metadata.NewMetricUint64("count"); err != nil {
		return err
	}

	if instance, err := e.Metadata.NewInstance("export"); err == nil {
		instance.SetLabel("task", "export")
	} else {
		return err
	}

	if instance, err := e.Metadata.NewInstance("render"); err == nil {
		instance.SetLabel("task", "render")
	} else {
		return err
	}

	e.SetStatus(0, "initialized")
	return nil
}

// GetClass returns the class of the AbstractExporter
func (e *AbstractExporter) GetClass() string {
	return e.Class
}

// GetName returns the name of the AbstractExporter
func (e *AbstractExporter) GetName() string {
	return e.Name
}

// GetExportedTotal returns the cumulative count of data points emitted since start.
// The poller derives the per-generation count as the delta against the previous generation,
// so reading never resets or mutates producer state and each count is attributed exactly once
func (e *AbstractExporter) GetExportedTotal() uint64 {
	return e.exportedTotal.Load()
}

// AddExportCount adds n to the cumulative exported total
func (e *AbstractExporter) AddExportCount(n uint64) {
	e.exportedTotal.Add(n)
}

// GetStatus returns the current state of the exporter. The returned code and reason
// always come from the same atomic state, so they never mismatch.
func (e *AbstractExporter) GetStatus() (uint8, string, string) {
	s := e.status.Load()
	return s.code, statusName[s.code], s.reason
}

// SetStatus atomically sets the current state of the exporter, with its matching reason
func (e *AbstractExporter) SetStatus(code uint8, msg string) {
	if code >= uint8(len(statusName)) {
		panic("invalid status code " + strconv.FormatUint(uint64(code), 10))
	}
	e.status.Store(&exporterStatus{code: code, reason: msg})
}

// @TODO: implement!
/*	This method/attribute is intended to tell Poller/collectors
	wheither or not they should metadata to this exporter.
	Currently not implemented
func (me *AbstractExporter) IsMaster() bool {
	return true
}
*/
/*
func (me *AbstractExporter) Export(data *matrix.Matrix) error {
	me.mu.Lock()
	defer me.mu.Unlock()
	return me.ExportData(data)
}

func (me *AbstractExporter) ExportData(data *matrix.Matrix) error {
	panic(me.Class + " did not implement ExportData()")
}

func (me *AbstractExporter) Render(data *matrix.Matrix) ([][]byte, error) {
	panic(me.Class + " did not implement Render()")
}*/
