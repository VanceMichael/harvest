# VictoriaMetrics Exporter

???+ note "VictoriaMetrics Install"

    The information below describes how to setup Harvest's VictoriaMetrics exporter.
    If you need help installing or setting up VictoriaMetrics, check
    out [their documentation](https://docs.victoriametrics.com/victoriametrics/).

## Overview

The VictoriaMetrics Exporter will format metrics into [Prometheus exposition format](https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#naming-restrictions).
The Exporter is compatible with VictoriaMetrics v1.129.1.

## Parameters

Overview of all parameters is provided below. Only one of `url` or `addr` should be provided and at least one of them is
required.
If `addr` is specified, it should be a valid TCP address or hostname of the VictoriaMetrics server and should not include the
scheme and port.

> `addr` only works with HTTP. If you need to use HTTPS, you should use `url` instead.

If `url` is specified, you must add all arguments to the url.
Harvest will do no additional processing and use exactly what you specify. (
e.g. `url: http://localhost:8428/api/v1/import/prometheus`.
When using `url`, the `addr` and `port` field will be ignored.

| parameter        | type                         | description                                                                                        | default |
|------------------|------------------------------|----------------------------------------------------------------------------------------------------|---------|
| `url`            | string                       | URL of the database, format: `SCHEME://HOST[:PORT]`                                                |         |
| `addr`           | string                       | address of the database, format: `HOST` (HTTP only)                                                |         |
| `port`           | int, optional                | port of the database                                                                               | `8086`  |
| `client_timeout` | int, optional                | client timeout in seconds                                                                          | `5`     |
| `sort_labels`    | bool, optional               | always emits metric labels in a deterministic order for this exporter. VictoriaMetrics requires this — without it, VictoriaMetrics will mark series stale if label order changes between polls. Setting `sort_labels: false` is ignored (with a logged warning), since it is never safe for VictoriaMetrics. | `true` (always)                      |

### Durable delivery

Rendered batches are spooled to disk before they are acknowledged by VictoriaMetrics and are removed only after a
successful write. Network failures, timeouts and server errors (HTTP 408/429/5xx) are retried in FIFO order with
deterministic exponential backoff, so a temporary outage no longer loses a collection round. Other 4xx errors are not
retried: the batch is moved to `<spool_path>/rejected/` and the exporter status becomes `rejected`. After recovery the
oldest batches are delivered before new snapshots. Each exporter instance has its own spool and sender goroutine; a
slow VictoriaMetrics never blocks other exporters, collectors or the poller schedule.

Exporter status, as visible in poller metadata (`metadata_component`), is one of: `up`, `backlogged` (batches queued),
`retrying` (head batch being retried), `rejected` (permanent 4xx on the latest batch) and `failed` (local spool
problem or backlog limit reached).

The optional `delivery` section tunes the pipeline (all fields optional):

| parameter                          | type          | description                                                                          | default        |
|------------------------------------|---------------|--------------------------------------------------------------------------------------|----------------|
| `delivery.spool_path`              | string        | base directory for per-poller/exporter spool sub-directories                        | `<harvest-home>/spool` |
| `delivery.queue_max`               | int           | unacknowledged batch payloads held in memory; overflow stays on disk                | `256`          |
| `delivery.spool_max_batches`       | int           | hard limit of unacknowledged batches on disk; new snapshots are refused past it     | `10000`        |
| `delivery.retry_min_interval`      | duration      | backoff of the first retry (`1s`, `500ms`, ...)                                     | `1s`           |
| `delivery.retry_max_interval`      | duration      | cap of the exponential backoff                                                      | `5m`           |

```yaml
Exporters:
  my_victoriametrics:
    exporter: VictoriaMetrics
    addr: localhost
    delivery:
      spool_path: /var/spool/harvest
      spool_max_batches: 20000
      retry_min_interval: 2s
      retry_max_interval: 10m
```

### Example

snippet from `harvest.yml` using `addr`: (supports HTTP only))

```yaml
Exporters:
  my_victoriametrics:
    exporter: VictoriaMetrics
    addr: localhost
    sort_labels: true
```

snippet from `harvest.yml` using `url`: (supports both HTTP/HTTPS))

```yaml
Exporters:
  victoriametrics2:
    exporter: VictoriaMetrics
    url: http://localhost:8428/api/v1/import/prometheus
    sort_labels: true
```

