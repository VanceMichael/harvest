# InfluxDB Exporter

???+ note "InfluxDB Install"

    The information below describes how to setup Harvest's InfluxDB exporter. 
    If you need help installing or setting up InfluxDB, check 
    out [their documentation](https://docs.influxdata.com/).

## Overview

The InfluxDB Exporter will format metrics into the
InfluxDB's [line protocol](https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#naming-restrictions)
and write it into a bucket.
The Exporter is compatible with InfluxDB v2.0.
For explanation about `bucket`, `org` and `precision`,
see [InfluxDB API documentation](https://docs.influxdata.com/influxdb/v2.0/api/#tag/Write).

If you are monitoring both CDOT and 7mode clusters, it is strongly recommended to use two different buckets.

## Parameters

Overview of all parameters is provided below. Only one of `url` or `addr` should be provided and at least one of them is
required.
If `addr` is specified, it should be a valid TCP address or hostname of the InfluxDB server and should not include the
scheme.  
When using `addr`, the `bucket`, `org`, and `token` key/values are required.

> `addr` only works with HTTP. If you need to use HTTPS, you should use `url` instead.

If `url` is specified, you must add all arguments to the url.
Harvest will do no additional processing and use exactly what you specify. (
e.g. `url: https://influxdb.example.com:8086/write?db=netapp&u=user&p=pass&precision=2`.
When using `url`, the `bucket`, `org`, `port`, and `precision` fields will be ignored.

| parameter        | type                         | description                                                                                        | default |
|------------------|------------------------------|----------------------------------------------------------------------------------------------------|---------|
| `url`            | string                       | URL of the database, format: `SCHEME://HOST[:PORT]`                                                |         |
| `addr`           | string                       | address of the database, format: `HOST` (HTTP only)                                                |         |
| `port`           | int, optional                | port of the database                                                                               | `8086`  |
| `bucket`         | string, required with `addr` | InfluxDB bucket to write                                                                           |         |
| `org`            | string, required with `addr` | InfluxDB organization name                                                                         |         |
| `precision`      | string, required with `addr` | Preferred timestamp precision in seconds                                                           | `2`     |
| `client_timeout` | int, optional                | client timeout in seconds                                                                          | `5`     |
| `token`          | string                       | [token for authentication](https://docs.influxdata.com/influxdb/v2.0/security/tokens/view-tokens/) |         |

### Durable delivery

Rendered batches are spooled to disk before they are acknowledged by InfluxDB and are removed only after a successful
write. Network failures, timeouts and server errors (HTTP 408/429/5xx) are retried in FIFO order with deterministic
exponential backoff, so a temporary outage no longer loses a collection round. Other 4xx errors are not retried: the
batch is moved to `<spool_path>/rejected/` and the exporter status becomes `rejected`. After recovery the oldest
batches are delivered before new snapshots. Each exporter instance has its own spool and sender goroutine; a slow
InfluxDB never blocks other exporters, collectors or the poller schedule.

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
  my_influx:
    exporter: InfluxDB
    addr: localhost
    bucket: harvest
    org: harvest
    token: my-token
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
  my_influx:
    exporter: InfluxDB
    addr: localhost
    bucket: harvest
    org: harvest
    token: ZTTrt%24@#WNFM2VZTTNNT25wZWUdtUmhBZEdVUmd3dl@# 
```

snippet from `harvest.yml` using `url`: (supports both HTTP/HTTPS))

```yaml
Exporters:
  influx2:
    exporter: InfluxDB
    url: https://localhost:8086/api/v2/write?org=harvest&bucket=harvest&precision=s
    token: my-token== 
```

Notice: InfluxDB stores a token in `~/.influxdbv2/configs`, but you can also retrieve it from the UI (usually serving
on `localhost:8086`): click on "Data" on the left task bar, then on "Tokens".
