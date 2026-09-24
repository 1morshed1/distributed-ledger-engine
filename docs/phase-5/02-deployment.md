# Phase 5 — Part 2: Packaging, CI, Kubernetes, Terraform

The path from a local binary to a cloud-deployable service. Local-first: the
Docker image and CI are exercised; k8s/Terraform are written and validated as
far as the local tooling allows (no cluster / no AWS here).

---

## 1. Dockerfile — multi-stage, tiny, non-root

`Dockerfile` builds a **fully static** binary (`CGO_ENABLED=0`) in a
`golang:1.26-alpine` stage, then copies just that binary onto `alpine:3.20`:

- deps are downloaded in their own layer (cached unless `go.mod` changes)
- final image runs as an unprivileged user (`uid 10001`)
- **result: 70.5 MB image**, verified with `docker build`

> Go version gotcha: a dependency (`validator/v10`) requires Go **1.26**, which
> bumped `go.mod`'s directive, so the build image must be `1.26`, not `1.24`.

`.dockerignore` keeps the build context small (no docs, spark, airflow, tf).

---

## 2. CI — GitHub Actions

`.github/workflows/ci.yml` runs on push / PR to `main`:

| Job     | What it does                                                        |
|---------|--------------------------------------------------------------------|
| `lint`  | `golangci-lint` (config in `.golangci.yml`; generated code excluded)|
| `test`  | spins a **Postgres service container**, runs migrations, `go test` |
| `build` | `docker build` the image (needs lint+test green)                   |

The `test` job points `DB_SOURCE` at the service Postgres — the same env var
`internal/db/main_test.go` reads — so the integration tests run against a real DB
in CI, exactly like locally.

---

## 3. Kubernetes manifests (`k8s/`)

Plain manifests + a `kustomization.yaml` (`kubectl apply -k k8s/`):

| Manifest         | Purpose                                                     |
|------------------|-------------------------------------------------------------|
| `namespace`      | `ledger` namespace                                          |
| `configmap`      | non-secret config (brokers, redis, OTLP endpoint)           |
| `secret`         | DB DSN + token key (dev values; prod uses External Secrets) |
| `deployment`     | 2 replicas, liveness/readiness on `/healthz`, CPU requests, Prometheus scrape annotations |
| `service`        | ClusterIP exposing http (8080) + grpc (9090)                |
| `hpa`            | scale 2→10 on 70% CPU                                        |

For a local run: `kind create cluster`, `kind load docker-image ledger-api:ci`,
`kubectl apply -k k8s/`. Backing stores stay in docker-compose; pods reach them
via `host.docker.internal` (set in the ConfigMap/Secret). All manifests are
YAML-validated; `kubectl`/`kind` aren't installed in this environment, so they're
applied on a cluster rather than here.

The HPA + `ledger_http_*` metrics + `ghz`/`k6` load testing (plan) close the loop:
drive load → watch p99 and replica count climb in Grafana.

---

## 4. Terraform (`terraform/`) — written, not applied

Declarative AWS stack, one concern per file, using the community
`terraform-aws-modules` for VPC/EKS:

- **VPC** (3 AZs, public/private, single NAT in dev)
- **EKS** with a **SPOT** node group (60-70% cheaper)
- **RDS Postgres** (`db.t3.micro`, free-tier), **ElastiCache Redis**, **MSK Kafka**
- **S3** reports bucket (versioned, 90-day → Glacier lifecycle)
- **S3 remote state** + DynamoDB lock in `backend.tf`

Cost controls per the plan keep dev under ~$50/mo (spot, micro instances, single
NAT, `terraform destroy` when idle). `terraform` isn't installed here and this is
deliberately **not applied** — see `terraform/README.md` for the apply workflow.

---

## Phase 5 complete (local scope)

Observability (metrics/traces/logs) ✅ · Dockerfile + CI ✅ · k8s manifests ✅ ·
Terraform ✅. The AWS deploy is codified and ready when you want to spend the
money; everything else runs free on docker-compose + a local kind cluster.
