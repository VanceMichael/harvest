package victoriametrics

import (
	"bytes"
	"context"
	"fmt"
	"github.com/netapp/harvest/v2/cmd/exporters"
	"github.com/netapp/harvest/v2/cmd/poller/exporter"
	"github.com/netapp/harvest/v2/cmd/poller/exporter/delivery"
	"github.com/netapp/harvest/v2/pkg/errs"
	"github.com/netapp/harvest/v2/pkg/matrix"
	"github.com/netapp/harvest/v2/pkg/requests"
	"github.com/netapp/harvest/v2/pkg/slogx"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPort       = 8428
	defaultTimeout    = 5
	defaultAPIVersion = "1"
	globalPrefix      = ""
)

type VictoriaMetrics struct {
	*exporter.AbstractExporter
	client       *http.Client
	url          string
	addMetaTags  bool
	sortLabels   bool
	globalPrefix string
	queue        *delivery.Queue
}

func New(abc *exporter.AbstractExporter) exporter.Exporter {
	return &VictoriaMetrics{AbstractExporter: abc}
}

func (v *VictoriaMetrics) Init() error {

	if err := v.InitAbc(); err != nil {
		return err
	}

	var (
		url, addr, version *string
		port               *int
	)

	if instance, err := v.Metadata.NewInstance("http"); err == nil {
		instance.SetLabel("task", "http")
	} else {
		return err
	}

	if instance, err := v.Metadata.NewInstance("info"); err == nil {
		instance.SetLabel("task", "info")
	} else {
		return err
	}

	if x := v.Params.GlobalPrefix; x != nil {
		v.Logger.Debug("use global prefix", slog.String("prefix", *x))
		v.globalPrefix = *x
		if !strings.HasSuffix(v.globalPrefix, "_") {
			v.globalPrefix += "_"
		}
	} else {
		v.globalPrefix = globalPrefix
	}

	// VictoriaMetrics keys a series off the raw label string, so sorting is not optional here
	v.sortLabels = true
	if v.Params.SortLabels != nil && !*v.Params.SortLabels {
		v.Logger.Warn("ignoring sort_labels: false, VictoriaMetrics requires deterministic label order")
	}

	// Checking the required/optional params
	// customer should either provide url or addr
	// url is expected to be the full write URL api/v1/import/prometheus
	// when url is defined, addr and port are ignored

	// addr is expected to include host only (no port)
	// when addr is defined, port is required

	dbEndpoint := "addr"
	if url = v.Params.URL; url != nil {
		v.url = *url
		dbEndpoint = "url"
	} else {
		if addr = v.Params.Addr; addr == nil {
			v.Logger.Error("missing url or addr")
			return errs.New(errs.ErrMissingParam, "url or addr")
		}
		if port = v.Params.Port; port == nil {
			v.Logger.Debug("using default port", slog.Int("default", defaultPort))
			port = new(defaultPort)
		}
		if version = v.Params.Version; version == nil {
			version = new(defaultAPIVersion)
		}
		v.Logger.Debug("using api version", slog.String("version", *version))

		//goland:noinspection HttpUrlsUsage
		url = new("http://" + *addr + ":" + strconv.Itoa(*port))
		v.url = fmt.Sprintf("%s/api/v%s/import/prometheus", *url, *version)
	}

	// timeout parameter
	timeout := time.Duration(defaultTimeout) * time.Second
	if ct := v.Params.ClientTimeout; ct != nil {
		if t, err := strconv.Atoi(*ct); err == nil {
			timeout = time.Duration(t) * time.Second
		} else {
			v.Logger.Warn(
				"invalid client_timeout, using default",
				slog.String("client_timeout", *ct),
				slog.Int("default", defaultTimeout),
			)
		}
	} else {
		v.Logger.Debug("using default client_timeout", slog.Int("default", defaultTimeout))
	}

	v.Logger.Debug("initializing exporter", slog.String("endpoint", dbEndpoint), slog.String("url", v.url))

	// construct HTTP client
	v.client = &http.Client{Timeout: timeout}

	// In test mode Export only renders metrics, no spool or sender needed.
	if !v.Options.IsTest {
		queue, err := delivery.Configure(
			v.Options.Poller, v.Class, v.Name, v.Params.Delivery, v.Logger, v.push, v.SetStatus,
		)
		if err != nil {
			return err
		}
		v.queue = queue
		queue.Start()
	}

	return nil
}

func (v *VictoriaMetrics) Export(data *matrix.Matrix) (exporter.Stats, error) {

	var (
		metrics [][]byte
		err     error
		s       time.Time
		stats   exporter.Stats
	)

	v.Lock()
	defer v.Unlock()

	s = time.Now()

	// update timestamp when backfill historical data, Ex: time.Now().Add(-1*24*time.Hour)
	timestamp := strconv.FormatInt(time.Now().Unix(), 10) // Ex: "1762933202"

	// render metrics into open metrics format with timestamp
	metrics, stats, _ = exporters.Render(data, v.addMetaTags, v.sortLabels, v.globalPrefix, v.Logger, timestamp)

	// fix render time
	v.Metadata.MustAddValueInt64("time", v.Metadata.MustGetInstance("render"), time.Since(s).Microseconds())
	// in test mode, don't emit metrics
	if v.Options.IsTest {
		return stats, nil
	}
	// spool the snapshot: it is sent and retried by the per-exporter
	// delivery goroutine and survives remote outages/restarts
	if err = v.enqueue(metrics, data); err != nil {
		return stats, fmt.Errorf("unable to queue object: %s, uuid: %s, err=%w", data.Object, data.UUID, err)
	}

	v.Logger.Debug(
		"exported",
		slog.String("object", data.Object),
		slog.String("uuid", data.UUID),
		slog.Int("numMetric", len(metrics)),
	)

	// update metadata
	v.Metadata.MustSetValueInt64("time", v.Metadata.MustGetInstance("export"), time.Since(s).Microseconds())

	// publish pipeline state with the exporter metadata batch
	v.updateDeliveryStats()

	// render metadata metrics into open metrics format with timestamp
	metrics, _, _ = exporters.Render(v.Metadata, v.addMetaTags, v.sortLabels, v.globalPrefix, v.Logger, timestamp)
	if err = v.enqueue(metrics, v.Metadata); err != nil {
		v.Logger.Error("queue metadata", slogx.Err(err))
	}

	return stats, nil
}

// enqueue durably queues one rendered matrix. Enqueue never performs a
// remote call, so a slow VictoriaMetrics cannot block the collector or any
// other exporter.
func (v *VictoriaMetrics) enqueue(metrics [][]byte, data *matrix.Matrix) error {
	if len(metrics) == 0 {
		return nil
	}
	return v.queue.Enqueue(bytes.Join(metrics, []byte("\n")), data.Object+"/"+data.UUID)
}

func (v *VictoriaMetrics) updateDeliveryStats() {
	snap := v.queue.Snapshot()
	v.SetDeliveryStats(uint64(snap.Pending), snap.Retries, snap.Rejected)
}

// push performs one delivery attempt of a spooled batch. It runs on the
// queue's single sender goroutine, never on a collector/poller goroutine.
func (v *VictoriaMetrics) push(ctx context.Context, batchID uint64, payload []byte) error {
	request, err := requests.New("POST", v.url, bytes.NewReader(payload))
	if err != nil {
		return delivery.RetryableError(err)
	}

	request.Header.Set(delivery.BatchHeader, strconv.FormatUint(batchID, 10))

	return delivery.Do(ctx, v.client, request)
}
