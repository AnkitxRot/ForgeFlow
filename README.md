# ForgeFlow

> **Distributed Durable Workflow & Job Execution Platform**  
> Designed for failure, concurrency, recovery, and distributed operational correctness.

[![Go Version](https://img.shields.io/badge/go-1.24%2B-blue.svg)](https://golang.org)
[![Architecture](https://img.shields.io/badge/architecture-Zero--Broker%20ACID-success.svg)](#core-architectural-pillars)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

---

## Overview

**ForgeFlow** is an open-source, high-integrity distributed job and workflow execution platform built in Go. It provides reliable execution of atomic background jobs and multi-step directed acyclic graph (DAG) workflows across pools of distributed workers.

ForgeFlow is built from first principles for mission-critical workloads where tasks must **never be lost, silently dropped, or executed out of order**.

---

## Core Architectural Pillars

1. **Zero Dual-Write (ACID Queue & State Engine)**:
   - Eliminates external message brokers (Redis, RabbitMQ, Kafka, NATS).
   - Unifies queue dispatch, job metadata, lease state, and execution history into a single transactional database engine using PostgreSQL `FOR UPDATE SKIP LOCKED`.
   - Supports pure-Go zero-CGO SQLite WAL mode for ultra-fast local development and continuous integration without external infrastructure dependencies.

2. **At-Least-Once Delivery & Three-Tier Monotonic Fencing**:
   - Explicit at-least-once delivery contract.
   - Every claim increments a monotonic `fencing_generation`, generates an unguessable UUID `lease_token`, and increments the `attempt` counter.
   - Completions, failures, and heartbeat renewals enforce strict fencing checks (`WHERE id = $1 AND fencing_generation = $2 AND lease_token = $3 AND status = 'RUNNING'`). Expired or late zombie workers attempting completion are rejected with `409 Conflict`.

3. **Declarative DAG Workflow Engine**:
   - Multi-step workflows defined declaratively as Directed Acyclic Graphs (JSON).
   - Validated at submission time using Three-Color Depth-First Search (DFS) for instant cycle detection.
   - Dynamic parameter interpolation piping step outputs into downstream step inputs (`{{ steps.<name>.output.<field> }}`) with 64KB bounded safety.

4. **Worker Security & Subprocess Isolation**:
   - Direct binary execution via OS `execve` (strictly avoids shell wrappers like `sh -c` or `cmd.exe /c` per ADR-005).
   - Automatic environment sanitization: strips database connection strings, cloud credentials, tokens, and sensitive host environment variables from child processes.
   - Process group limits and hard context cancellation timeouts prevent orphaned processes.

5. **Built-in Observability & Zero-Leak Telemetry**:
   - Zero-external-dependency Prometheus metrics exporter on `GET /metrics` reporting real-time queue depths, claim latency, job run durations, and worker state.
   - Structured JSON logging (`log/slog`) with automatic redaction of sensitive credentials (`api_key`, `token`, `password`, `secret`, `lease_token`).

---

## Milestone Status & Verification Matrix

| Milestone | Subsystem | Verification Level | Status |
|---|---|---|---|
| **M1** | Domain Foundation & SQLite Store | Race-tested unit & store tests (`CGO_ENABLED=0`) | **PROVEN** |
| **M2** | PostgreSQL Atomic Queue Engine | Live PG 16.15 concurrency stress test (100 workers, 0 duplicates) | **PROVEN** |
| **M3** | Worker Engine & Heartbeat Lease | Concurrent claim loop, lease renewal, subprocess runner | **PROVEN** |
| **M4** | Retry Engine & Lease Reaper | Full-jitter exponential backoff, background lease reaper | **PROVEN** |
| **M5** | Declarative DAG Engine | Three-Color DFS cycle validation, output parameter piping | **PROVEN** |
| **M6** | REST API & Authentication | RFC 9106 Argon2id API key auth (ADR-006), tenant isolation | **PROVEN** |
| **M7** | Unified CLI Binary | Self-contained single-binary `forgeflow` CLI | **PROVEN** |
| **M8** | Observability & Telemetry | Prometheus `/metrics` exposition, redacting JSON logger | **PROVEN** |
| **M9** | Chaos Engineering & Hardening | Worker crash recovery, zombie fencing, partition simulation | **PROVEN** |
| **M10** | Production Packaging | Dockerfile, docker-compose, unprivileged container specs | **PROVEN (Spec)** / **NOT_PROVEN (Host Runtime)** |

---

## Deployment Envelope

ForgeFlow supports two persistent operational models:

1. **Embedded / Single-Node (SQLite WAL)**:
   - Zero-dependency, pure-Go (`modernc.org/sqlite`) local persistence with single-writer serialization (`MaxOpenConns(1)`).
   - Designed for embedded agents, developer environments, edge services, and single-node orchestration.
   - Benchmark: **~3,100 jobs/sec** end-to-end claim and complete.

2. **Distributed / Multi-Node (PostgreSQL 15+)**:
   - Multi-worker concurrent queue dispatch backed by PostgreSQL atomic CTEs with `FOR UPDATE SKIP LOCKED`.
   - Designed for high-scale distributed worker pools, horizontal worker scaling, and enterprise durability.
   - Benchmark: **~4,300 ops/sec** multi-worker concurrent claim throughput with 100-worker contention safety.

---

## Quickstart Guide

### 1. Build from Source
```bash
# Build the unified forgeflow binary
go build -v -o bin/forgeflow ./cmd/forgeflow
```

### 2. Start the API Server
```bash
# Start API server using embedded SQLite (defaults to port :8080)
./bin/forgeflow server -addr=:8080 -db-type=sqlite -db=forgeflow.db

# Or with PostgreSQL
./bin/forgeflow server -addr=:8080 -db-type=postgres -db="postgres://postgres:postgres@localhost:5432/forgeflow?sslmode=disable"
```

### 3. Start a Worker Node
```bash
# Start a worker node listening on default and high-priority queues
./bin/forgeflow worker -id=worker-1 -tenant=default -queues=high-priority,default -concurrency=5 -db=forgeflow.db
```

### 4. Job Operations via CLI
```bash
# Submit a new job
./bin/forgeflow job submit \
  -queue=high-priority \
  -priority=10 \
  -payload='{"action":"generate_report","account_id":"acc_102"}'

# Query job status
./bin/forgeflow job get -id=job-a1b2c3d4e5f6

# Cancel a job
./bin/forgeflow job cancel -id=job-a1b2c3d4e5f6
```

### 5. Declarative DAG Workflow via CLI
```bash
# Submit a declarative multi-step DAG workflow
./bin/forgeflow workflow submit -file=examples/workflow.json

# Check workflow execution progress
./bin/forgeflow workflow get -id=wf-a1b2c3d4e5f6

# Cancel an active workflow
./bin/forgeflow workflow cancel -id=wf-a1b2c3d4e5f6
```

---

## Running with Docker & Docker Compose

Deploy a complete production-grade ForgeFlow cluster (PostgreSQL + API Server + Distributed Worker) with a single command:

```bash
# Copy example environment configuration
cp .env.example .env

# Start PostgreSQL, ForgeFlow API server, and Worker
docker compose up -d

# View real-time cluster logs
docker compose logs -f
```

---

## REST API Reference

All requests to `/api/v1/*` must include the `X-API-Key` HTTP header for authentication.

| Method | Endpoint | Description | Auth Required |
|---|---|---|---|
| `GET` | `/healthz` | Kubernetes liveness probe | No |
| `GET` | `/readyz` | Database connectivity readiness probe | No |
| `GET` | `/metrics` | Prometheus metrics exposition | No |
| `POST` | `/api/v1/jobs` | Submit a new background job | Yes |
| `GET` | `/api/v1/jobs/{id}` | Retrieve job state and execution status | Yes |
| `POST` | `/api/v1/jobs/{id}/cancel` | Cancel an active or pending job | Yes |
| `POST` | `/api/v1/workflows` | Submit a declarative DAG workflow | Yes |
| `GET` | `/api/v1/workflows/{id}` | Get workflow state and step executions | Yes |
| `POST` | `/api/v1/workflows/{id}/cancel` | Cancel a running DAG workflow | Yes |

### Example: Submit a Job via `curl`
```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: ff_live_secret_key_12345" \
  -d '{
    "queue_name": "payments",
    "priority": 5,
    "payload": { "transaction_id": "tx_99812", "amount": 4900 },
    "max_retries": 3,
    "timeout_seconds": 60
  }'
```

### Example: Submit a DAG Workflow via `curl`
```bash
curl -X POST http://localhost:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -H "X-API-Key: ff_live_secret_key_12345" \
  -d '{
    "name": "data-pipeline",
    "definition": {
      "name": "data-pipeline",
      "steps": [
        {
          "name": "extract",
          "handler": "extract-handler",
          "input": { "source": "s3://raw-bucket" }
        },
        {
          "name": "transform",
          "handler": "transform-handler",
          "dependencies": ["extract"],
          "input": { "raw_data": "{{ steps.extract.output.url }}" }
        },
        {
          "name": "load",
          "handler": "load-handler",
          "dependencies": ["transform"],
          "input": { "clean_data": "{{ steps.transform.output.clean_url }}" }
        }
      ]
    }
  }'
```

---

## Verification & Testing Guide

ForgeFlow enforces strict concurrency and race testing standards. Every component is verified using Go's built-in race detector and pure-Go execution modes:

```bash
# Run all unit and integration tests with data race detector
go test -v -race ./...

# Run pure-Go SQLite tests without CGO
$env:CGO_ENABLED="0"; go test -v ./...

# Run the distributed chaos and fault-injection suite
go test -v -race ./tests/chaos/...

# Verify static code invariants
go vet ./...
```

---

## Contributing & Development Guidelines

For coding conventions, architectural invariants, security rules, and contributor standards, consult **[CLAUDE.md](CLAUDE.md)**.
