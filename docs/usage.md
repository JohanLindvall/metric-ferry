# Metric Ferry usage reference

[Back to the quick start](../README.md)

Run the commands below from the repository root after building `metric-ferry`.
Replace `metrics.om.zst` with your compressed history.

## Import

```sh
# Validate and encode both formats without sending anything.
./metric-ferry -input metrics.om.zst -dry-run

# Prometheus remote write. Supply the COMPLETE receiver URL.
./metric-ferry -input metrics.om.zst \
  -prometheus-url http://localhost:9090/api/v1/write

# An OpenTelemetry Collector or another OTLP HTTP receiver.
./metric-ferry -input metrics.om.zst \
  -otlp-url http://localhost:4318/v1/metrics

# Send to both in one decoding pass, with independent compressed requests.
./metric-ferry -input metrics.om.zst \
  -prometheus-url http://localhost:9090/api/v1/write \
  -otlp-url http://localhost:4318/v1/metrics \
  -prometheus-header "Authorization=Bearer $PROM_TOKEN" \
  -otlp-header "Authorization=Bearer $OTLP_TOKEN"
```

For Prometheus itself, enable its
[`--web.enable-remote-write-receiver`](https://prometheus.io/docs/prometheus/latest/command-line/prometheus/)
flag. Use an empty/dedicated destination for historical imports when possible.
The receiver's retention, age, out-of-order, label, and request-size limits still
apply. Historical data is sent as fast as the receiver accepts it; this tool does
not replay data at wall-clock speed.

`-dry-run` uses the same parsing, validation, mapping, serialization and
compression paths, without network requests. With no destination flags it encodes
both protocols; specify one destination with `-dry-run` to exercise only that
protocol. `-limit 1000000` processes a prefix for a quick trial. A limited run does
not validate the remainder of the archive or its final checksum.

## Timestamp rebasing

OpenMetrics timestamps are decimal **seconds**. Decimal conversion preserves
nanosecond precision; remote write stores milliseconds and OTLP stores
nanoseconds. For Prometheus text exports with integer millisecond timestamps, set
`-timestamp-unit ms`. Supported units: `s`, `ms`, `us`, `ns`.

```sh
# Move the NEWEST sample to now. Earlier samples retain their relative timing.
./metric-ferry -input metrics.om.zst \
  -prometheus-url http://localhost:9090/api/v1/write -rebase now

# Anchor to a fixed end time for reproducible imports.
./metric-ferry -input metrics.om.zst \
  -otlp-url http://localhost:4318/v1/metrics \
  -rebase 2026-10-20T12:00:00Z

# One pass: move the FIRST encountered sample to a chosen time.
./metric-ferry -input metrics.om.zst \
  -otlp-url http://localhost:4318/v1/metrics \
  -rebase 2026-10-20T10:00:00Z -rebase-anchor first

# One pass: apply a fixed offset.
./metric-ferry -input metrics.om.zst \
  -prometheus-url http://localhost:9090/api/v1/write -shift 240h

# Stdin supports a fixed shift or first-sample rebasing.
cat metrics.om.zst | ./metric-ferry -input - \
  -otlp-url http://localhost:4318/v1/metrics -shift=-1h
```

`-rebase-anchor end` is the default. It scans the file to find the maximum timestamp,
then reopens it for import, without materializing decompressed data. `now` is
captured after that scan. With `-limit`, the maximum is calculated over the same
input prefix that will be imported. Do not modify the file between passes.

`first` means the first encountered sample, which is not necessarily the earliest.
Rebasing the first sample to `now` puts subsequent samples in the future, so use
the default end anchor when importing a history up to the present. All timestamps
receive the same shift; values, including metrics whose *values* represent times,
are unchanged. Rebasing and `-shift` are mutually exclusive. Timestamp arithmetic
outside signed 64-bit nanoseconds is rejected.

## OpenTelemetry attributes

The default `-attribute-mode semantic` restores **known** semantic attribute keys,
including underscores that belong inside a name component:

| Input label | OTLP key | Placement |
| --- | --- | --- |
| `service_name` | `service.name` | Resource |
| `service_namespace` | `service.namespace` | Resource |
| `service_instance_id` | `service.instance.id` | Resource |
| `k8s_cluster_name` | `k8s.cluster.name` | Resource |
| `k8s_pod_name` | `k8s.pod.name` | Resource |
| `container_id` | `container.id` | Resource |
| `deployment_environment_name` | `deployment.environment.name` | Resource |
| `deployment_environment` | `deployment.environment.name` | Resource |
| `cloud_availability_zone` | `cloud.availability_zone` | Resource |
| `cloud_resource_id` | `cloud.resource_id` | Resource |
| `http_response_status_code` | `http.response.status_code` | Data point |
| `network_protocol_name` | `network.protocol.name` | Data point |
| `server_address` | `server.address` | Data point |
| `cache_type` | `cache_type` | Data point |

The explicit lists live in [`mapping.go`](../mapping.go), following
[OpenTelemetry attribute naming](https://opentelemetry.io/docs/specs/semconv/general/naming/)
and [resource conventions](https://opentelemetry.io/docs/specs/semconv/resource/).
Prometheus label values remain strings, including attributes for which a native
OTel producer might use integers or arrays.

Unknown application labels stay unchanged. Plain `namespace`, `pod`, `host`,
`method`, and similar ambiguous labels are not guessed. `job=namespace/service`
supplies missing `service.namespace` and `service.name`; `instance` supplies a
missing `service.instance.id`. Explicit service attributes take precedence. The
original `job` and `instance` also remain as data-point dimensions to preserve
source identity.

Both environment aliases may be present if their values agree. Conflicting
aliases fail with an error. A mapping that merges two distinct source series also
fails, instead of silently combining their values.

```sh
./metric-ferry -input metrics.om.zst \
  -otlp-url http://localhost:4318/v1/metrics \
  -rename cache_type=cache.type \
  -rename platform_product_name=platform.product.name \
  -resource-attribute platform.product.name

# Custom platform/Atlas resource rules for this dataset:
./metric-ferry -input metrics.om.zst \
  -otlp-url http://localhost:4318/v1/metrics \
  -mapping-file mapping.example.json
```

Mapping JSON has `rename` (source-to-final-key object), `resource` (final keys),
and `datapoint` (final keys). Repeat `-rename`, `-resource-attribute`, and
`-datapoint-attribute` as needed. CLI renames override file renames.
`-datapoint-attribute k8s.pod.name` overrides automatic resource promotion.

`-attribute-mode all` additionally replaces every underscore in unknown keys with
a dot; explicit/known rules still take precedence. `-attribute-mode none` disables
automatic renaming and job/instance fallback; explicit rules and promotion of
already dotted, recognized resource attributes still work.

Metric names and Prometheus destination labels retain their original names.

## Metric types and input contract

This tool imports timestamped scalar OpenMetrics/Prometheus text samples, including
escaped labels, `NaN`, and infinities. Explicit sample timestamps are required.
It accepts histories with repeated series and an optional terminal `# EOF`.

- `# TYPE ... counter` becomes an OTLP cumulative monotonic sum.
- Untyped names ending in `_total` also become sums by default. Set
  `-infer-counters=false` to use gauges when type metadata is absent.
- Other samples become OTLP gauges. `# HELP` and `# UNIT` metadata on scalar
  families are carried into OTLP, with common units converted to UCUM.
- Histogram buckets, sums, counts and summary quantiles are **preserved as separate
  scalar series**. They are not reconstructed into native OTLP histogram or summary
  points. This also preserves historical exports that contain no type metadata.
- Counter start timestamps are unknown and left unset. `_created` samples remain
  separate scalar series. Counter resets remain decreases in cumulative values.
- Exemplars are rejected unless `-drop-exemplars` explicitly permits discarding
  them. Native histogram input is not supported.

These choices preserve the scalar history while supporting the relevant parts of
the [Prometheus-to-OTLP mapping](https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/);
this is not a complete OpenMetrics scraper/translator.

Input must be timestamp-ordered **within each series**. Global ordering between
different series is unnecessary. Label order does not affect identity; empty
labels are normalized away. Duplicate identical samples are skipped. Conflicting
values at the same destination timestamp, including two different values inside
one Prometheus millisecond, are rejected.

## Throughput and delivery

The decoder streams into a reader with a bounded line size. A capped cache parses
each distinct text label set and computes its OTLP mapping once. Samples are hashed
into fixed worker queues; every series always uses the same queue. Each queue sends
its requests sequentially, including retries, so concurrent requests cannot reorder
a series. The two destinations can run concurrently.

Remote write uses grouped series, protobuf and Snappy **block** compression.
OTLP uses resource/metric grouping, protobuf, reused point storage, and fast gzip.
HTTP connections and compression buffers are reused.

| Flag | Default | Purpose |
| --- | --- | --- |
| `-workers` | up to 4 CPUs | Parallel ordered shards per destination |
| `-batch-size` | `10000` | Maximum input samples per shard batch |
| `-batch-bytes` | `4194304` | Maximum uncompressed protobuf request size |
| `-zstd-workers` | up to 4 CPUs | Native zstd decoder concurrency |
| `-series-limit` | `200000` | Cap cached source series/text label sets |
| `-max-line-bytes` | `4194304` | Maximum decompressed line size |
| `-otlp-compression` | `gzip` | `gzip` or `none` |
| `-retries` | `8` | Retries after the initial request |
| `-timeout` | `30s` | Timeout per HTTP attempt |
| `-progress` | `5s` | Reporting interval; `0` disables periodic reports |

Increase workers if the receiver/network is the bottleneck; more workers also use
more memory. Try `-otlp-compression none` on a fast local network if compression
CPU dominates. The byte budget also triggers approximate early batch flushing;
serialized size is checked before sending. HTTP 413 responses trigger ordered
batch splitting. A single sample that cannot fit is an error.

The cache deliberately fails at `-series-limit` rather than evicting ordering
state. Increase that limit for higher cardinality, with a corresponding memory
cost. Memory depends on label sizes, cache cardinality, decoder window, workers,
and batch sizes, rather than decompressed archive length. The zstd decoder has a
256 MiB memory/window limit.

Retryable network errors and receiver status codes use exponential backoff with
jitter and respect `Retry-After`. Permanent errors and OTLP partial rejection
stop the import. Partial-success responses are never replayed. Redirects are not
followed. The protocol behavior follows
[remote write](https://prometheus.io/docs/specs/prw/remote_write_spec/) and
[OTLP HTTP](https://opentelemetry.io/docs/specs/otlp/#otlphttp).

Delivery is not transactional or exactly once. Previously accepted batches stay
accepted if a later parse, checksum, HTTP, or second-destination error occurs.
An ambiguous network failure may cause a batch to be replayed. There is no
persistent checkpoint/resume mechanism. Run `-dry-run` first to validate an archive
before writing it.

Progress reports count processed input samples, accepted/encoded input points by
destination, requests, compressed bytes and retries. In dry runs those destination
counts mean encoded, not delivered. Identical sub-millisecond points can collapse
into one Prometheus sample, so the Prometheus input-point count can exceed the wire
sample count.

In a previously recorded local run on an archive (186 MiB compressed, 17.8 GiB decompressed), a full dry run
encoded **20,736,220 samples / 30,698 series for both destinations in 70.9 seconds**
on an Intel Core Ultra 9 185H, with about **649 MiB peak RSS** and default options.
That is approximately 293,000 input samples/second, including both serialization
and compression paths. This measures local processing, not receiver ingestion
throughput. The archive is not included in this repository, so this result is
context rather than a reproducible benchmark fixture or a performance guarantee.
See [benchmarking](benchmarks.md) to measure your own data. The cached parser
benchmark performs no allocations per sample.
