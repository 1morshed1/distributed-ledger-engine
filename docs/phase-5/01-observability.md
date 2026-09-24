# Phase 5 — Part 1: Observability (Metrics, Traces, Logs)

Code: `internal/observability/*`, wired in `internal/api/server.go` and
`cmd/api/main.go`. Infra: `docker-compose.yml` + `observability/*`.

The three pillars, all working locally against the host-run API.

---

## 1. Metrics — Prometheus + Grafana

`internal/observability/metrics.go` registers Prometheus metrics and a Gin
middleware:

- `ledger_http_requests_total{method,path,status}` — request counter
- `ledger_http_request_duration_seconds{method,path}` — latency histogram
  (p50/p95/p99 derivable via `histogram_quantile`)
- `ledger_transfers_total{outcome}` — business metric

**Cardinality guard**: the `path` label uses the *route template*
(`c.FullPath()`, e.g. `/api/v1/accounts/:id`), not the raw URL. Labeling by raw
URL would create a new time series per account id and blow up Prometheus.

`GET /metrics` exposes them. Prometheus scrapes it every 10s. Since the API runs
on the host, Prometheus reaches it via `host.docker.internal` (mapped through
`extra_hosts: host-gateway`). Verified: `up{job="ledger-api"} == 1`.

Grafana (`:3000`, anonymous viewing on) is auto-provisioned with Prometheus + Loki
datasources.

---

## 2. Traces — OpenTelemetry + Jaeger

`internal/observability/tracing.go` sets up an OTLP/HTTP exporter to Jaeger and a
W3C trace-context propagator. `otelgin` middleware opens a span per request.

**The payoff — one transaction, end to end.** Every request gets a trace id. We
put that id into the Kafka event's `metadata.trace_id`
(`trace.SpanContextFromContext(...).TraceID()` in the transfer handlers), so the
Spark fraud job can log the same id. You can follow a single transfer:

```
REST/gRPC request (span) ──► Kafka event (metadata.trace_id) ──► Spark log
```

Verified: Jaeger UI (`:16686`) shows the `ledger-api` service with traces.

`InitTracing` is a no-op if `OTLP_ENDPOINT` is empty, so the app runs fine without
a collector.

---

## 3. Logs — structured JSON + Loki

`internal/observability/logging.go` installs a JSON `slog` handler as the default
logger. JSON is what log stores query by field. Verified in the boot logs:

```json
{"time":"...","level":"INFO","msg":"starting gRPC server on 0.0.0.0:9090"}
```

**Promtail** tails Docker container logs (via the Docker socket) and ships them to
**Loki** (`:3100`). Verified: Loki has logs from 12 containers, queryable by
`container` label in Grafana. When the API is containerized (next: Dockerfile), its
JSON logs flow here too and can be joined to traces by `trace_id`.

---

## 4. Ports (local)

| Service     | URL                     | Notes                          |
|-------------|-------------------------|--------------------------------|
| Prometheus  | http://localhost:9091   | 9090 is taken by s3mock        |
| Grafana     | http://localhost:3000   | admin/admin (anon view on)     |
| Jaeger UI   | http://localhost:16686  | OTLP on 4318/4317              |
| Loki        | http://localhost:3100   | queried via Grafana            |

> Port note: the app's gRPC uses 9090, so s3mock's host port was moved to 9095.
> Inside the compose network s3mock is still `s3mock:9090`.

---

## Try it

```bash
make up                 # includes jaeger/prometheus/grafana/loki/promtail
make server             # host-run API exports traces + metrics
# generate traffic, then:
open http://localhost:16686   # traces
open http://localhost:9091    # prometheus  (query: ledger_http_requests_total)
open http://localhost:3000    # grafana
```

---

## Next

Phase 5 continues: Dockerfile + GitHub Actions CI, Kubernetes manifests (kind),
Terraform (write-only).
