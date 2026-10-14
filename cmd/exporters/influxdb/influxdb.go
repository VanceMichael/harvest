/*
 * Copyright NetApp Inc, 2021 All rights reserved
 */

package influxdb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/netapp/harvest/v2/cmd/poller/exporter"
	"github.com/netapp/harvest/v2/cmd/poller/exporter/delivery"
	"github.com/netapp/harvest/v2/pkg/errs"
	"github.com/netapp/harvest/v2/pkg/matrix"
	"github.com/netapp/harvest/v2/pkg/requests"
	"github.com/netapp/harvest/v2/pkg/slogx"
	"log/slog"
	"net/http"
	url2 "net/url"
	"strconv"
	"strings"
	"time"
)

/* Write metrics to the InfluxDB Time-Series Database.
   The exporter follows InfluxDB's v2 documentation
   for authentication and line protocol (measurement):

   - https://docs.influxdata.com/influxdb/v2.0/write-data/developer-tools/api/
   - https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/

*/

const (
	defaultPort         = 8086
	defaultTimeout      = 5
	defaultAPIVersion   = "2"
	defaultAPIPrecision = "s"
)

// some field names that we need to avoid
// first two: to avoid collision with label names
// others: protected field names by influxdb
var protectedFieldNames = map[string]string{
	"status":       "status_code",
	"new_status":   "new_status_code",
	"time":         "harvest_time",
	"_measurement": "harvest_measurement",
	"_field":       "harvest_field",
	"expiry_time":  "harvest_expiry_time",
}

type InfluxDB struct {
	*exporter.AbstractExporter
	client *http.Client
	url    string
	token  string
	queue  *delivery.Queue
}

func New(abc *exporter.AbstractExporter) exporter.Exporter {
	return &InfluxDB{AbstractExporter: abc}
}

func (e *InfluxDB) Init() error {

	if err := e.InitAbc(); err != nil {
		return err
	}

	var (
		url, addr, bucket, org, token, version, precision *string
		port                                              *int
	)

	// check the required / optional parameters
	// customer should provide either url or addr
	// url is expected to be the full write URL with all query params specified (optionally with scheme)
	// when url is defined, addr, bucket, org, port, and precision are ignored

	// addr is expected to include host only (no scheme, no port)
	// when addr is defined, bucket, org, port, and precision are required

	dbEndpoint := "addr"
	if url = e.Params.URL; url != nil {
		e.url = *url
		dbEndpoint = "url"
	} else {
		if addr = e.Params.Addr; addr == nil {
			return errs.New(errs.ErrMissingParam, "url or addr")
		}
		if port = e.Params.Port; port == nil {
			e.Logger.Debug("using default port", slog.Int("default", defaultPort))
			port = new(defaultPort)
		}
		if version = e.Params.Version; version == nil {
			version = new(defaultAPIVersion)
		}
		e.Logger.Debug("using api version", slog.String("version", *version))

		if bucket = e.Params.Bucket; bucket == nil {
			return errs.New(errs.ErrMissingParam, "bucket")
		}
		e.Logger.Debug("using bucket", slog.String("bucket", *bucket))

		if org = e.Params.Org; org == nil {
			return errs.New(errs.ErrMissingParam, "org")
		}
		e.Logger.Debug("using organization", slog.String("org", *org))

		if precision = e.Params.Precision; precision == nil {
			precision = new(defaultAPIPrecision)
		}
		e.Logger.Debug("using api precision", slog.String("precision", *precision))

		//goland:noinspection HttpUrlsUsage
		url = new("http://" + *addr + ":" + strconv.Itoa(*port))
		e.url = fmt.Sprintf("%s/api/v%s/write?org=%s&bucket=%s&precision=%s",
			*url, *version, url2.PathEscape(*org), url2.PathEscape(*bucket), *precision)
	}

	if token = e.Params.Token; token == nil {
		return errs.New(errs.ErrMissingParam, "token")
	}
	e.token = *token
	e.Logger.Debug("will use authorization with api token")

	// timeout parameter
	timeout := time.Duration(defaultTimeout) * time.Second
	if ct := e.Params.ClientTimeout; ct != nil {
		if t, err := strconv.Atoi(*ct); err == nil {
			timeout = time.Duration(t) * time.Second
		} else {
			e.Logger.Warn(
				"invalid client_timeout, using default",
				slog.String("client_timeout", *ct),
				slog.Int("default", defaultTimeout),
			)
		}
	} else {
		e.Logger.Debug("using default client_timeout", slog.Int("default", defaultTimeout))
	}

	e.Logger.Debug("initializing exporter", slog.String("endpoint", dbEndpoint), slog.String("url", e.url))

	// construct HTTP client
	e.client = &http.Client{Timeout: timeout}

	// In test mode Export only renders metrics, no spool or sender needed.
	if !e.Options.IsTest {
		queue, err := delivery.Configure(
			e.Options.Poller, e.Class, e.Name, e.Params.Delivery, e.Logger, e.push, e.SetStatus,
		)
		if err != nil {
			return err
		}
		e.queue = queue
		queue.Start()
	}

	return nil
}

func (e *InfluxDB) Export(data *matrix.Matrix) (exporter.Stats, error) {

	var (
		metrics [][]byte
		err     error
		s       time.Time
		stats   exporter.Stats
	)

	e.Lock()
	defer e.Unlock()

	s = time.Now()

	// render the metrics, i.e. convert to InfluxDb line protocol
	if metrics, stats, err = e.Render(data); err == nil && len(metrics) != 0 {
		// fix render time
		e.Metadata.MustAddValueInt64("time", e.Metadata.MustGetInstance("render"), time.Since(s).Microseconds())
		// in test mode, don't emit metrics
		if e.Options.IsTest {
			return stats, nil
		}
		// spool the snapshot: it is sent and retried by the per-exporter
		// delivery goroutine and survives remote outages/restarts
		if err = e.enqueue(metrics, data); err != nil {
			return stats, fmt.Errorf("unable to queue object: %s, uuid: %s, err=%w", data.Object, data.UUID, err)
		}
	}

	e.Logger.Debug(
		"exported",
		slog.String("object", data.Object),
		slog.String("uuid", data.UUID),
		slog.Int("numMetric", len(metrics)),
	)

	// update metadata
	e.Metadata.MustSetValueInt64("time", e.Metadata.MustGetInstance("export"), time.Since(s).Microseconds())

	if e.Options.IsTest {
		return stats, nil
	}

	// publish pipeline state with the exporter metadata batch
	e.updateDeliveryStats()
	if metrics, _, err = e.Render(e.Metadata); err != nil {
		e.Logger.Error("render metadata", slogx.Err(err))
	} else if err = e.enqueue(metrics, e.Metadata); err != nil {
		e.Logger.Error("queue metadata", slogx.Err(err))
	}

	return stats, nil
}

// enqueue durably queues one rendered matrix. Enqueue never performs a
// remote call, so a slow InfluxDB cannot block the collector or any other
// exporter.
func (e *InfluxDB) enqueue(metrics [][]byte, data *matrix.Matrix) error {
	if len(metrics) == 0 {
		return nil
	}
	return e.queue.Enqueue(bytes.Join(metrics, []byte("\n")), data.Object+"/"+data.UUID)
}

func (e *InfluxDB) updateDeliveryStats() {
	snap := e.queue.Snapshot()
	e.SetDeliveryStats(uint64(snap.Pending), snap.Retries, snap.Rejected)
}

// push performs one delivery attempt of a spooled batch. It runs on the
// queue's single sender goroutine, never on a collector/poller goroutine.
func (e *InfluxDB) push(ctx context.Context, batchID uint64, payload []byte) error {
	request, err := requests.New("POST", e.url, bytes.NewReader(payload))
	if err != nil {
		return delivery.RetryableError(err)
	}

	request.Header.Set("Authorization", "Token "+e.token)
	request.Header.Set(delivery.BatchHeader, strconv.FormatUint(batchID, 10))

	return delivery.Do(ctx, e.client, request)
}

func (e *InfluxDB) Render(data *matrix.Matrix) ([][]byte, exporter.Stats, error) {

	var (
		count, countTmp, instancesExported uint64
	)

	rendered := make([][]byte, 0)

	object := data.Object

	// user-defined preferences for export
	var labelsToInclude, keysToInclude []string
	includeAll := data.GetExportOptions().GetChildContentS("include_all_labels") == "true"
	if x := data.GetExportOptions().GetChildS("instance_keys"); x != nil {
		keysToInclude = x.GetAllChildContentS()
	}
	if x := data.GetExportOptions().GetChildS("instance_labels"); x != nil {
		labelsToInclude = x.GetAllChildContentS()
	}

	// measurement that we will not emit
	// only to store global labels that we'll
	// add to all instances
	global := NewMeasurement("", 0)
	for key, value := range data.GetGlobalLabels() {
		global.AddTag(key, value)
	}

	// render one measurement for each instance
	for key, instance := range data.GetInstances() {

		countTmp = 0

		if !instance.IsExportable() {
			continue
		}

		instancesExported++

		m := NewMeasurement(object, len(global.tagSet))
		copy(m.tagSet, global.tagSet)

		// tag set
		if includeAll {
			for label, value := range instance.GetLabels() {
				if value != "" {
					m.AddTag(label, value)
				}
			}
		} else {
			for _, key := range keysToInclude {
				if value, has := instance.GetLabels()[key]; has && value != "" {
					m.AddTag(key, value)
				}
			}
		}

		// skip instance without key tags
		if len(m.tagSet) == 0 {
			e.Logger.Debug(
				"skip instance, no tag set parsed from labels",
				slog.String("key", key),
				slog.Any("labels", instance.GetLabels()),
			)
		}

		// field set

		// strings
		for _, label := range labelsToInclude {
			if value, has := instance.GetLabels()[label]; has && value != "" {
				if value == "true" || value == "false" {
					m.AddField(label, value)
				} else {
					m.AddFieldString(label, value)
				}

				countTmp++
			}
		}

		// numeric
		for _, metric := range data.GetMetrics() {

			if !metric.IsExportable() {
				continue
			}

			value, ok := metric.GetValueString(instance)

			if !ok {
				continue
			}

			fieldName := metric.GetName()
			var fnb strings.Builder
			fnb.WriteString(fieldName)

			if metric.HasLabels() {
				for _, label := range metric.GetLabels() {
					fnb.WriteString("_" + label)
				}
				fieldName = fnb.String()
			}

			if rename, has := protectedFieldNames[fieldName]; has {
				fieldName = rename
			}

			m.AddField(fieldName, value)
			countTmp++
		}

		// skip instance with no tag set (no metrics)
		if len(m.fieldSet) == 0 {
			e.Logger.Debug("skip instance, no field set parsed", slog.Any("instance", instance))
		} else if r, err := m.Render(); err == nil {
			rendered = append(rendered, []byte(r))
			count += countTmp
		} else {
			e.Logger.Debug(err.Error())
		}
	}

	e.Logger.Debug("rendered", slog.Int("instances", len(rendered)), slog.Uint64("metrics", count))

	// update metadata
	e.AddExportCount(count)
	e.Metadata.MustSetValueUint64("count", e.Metadata.MustGetInstance("export"), count)
	return rendered, exporter.Stats{InstancesExported: instancesExported, MetricsExported: count}, nil
}
