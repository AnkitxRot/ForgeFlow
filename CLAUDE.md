# CLAUDE.md - Coding Agent Guidelines for JobEngine

## Repository Purpose
JobEngine is a distributed, durable workflow and job execution engine written in Go. It enables client applications to submit atomic tasks and multi-step DAG workflows that execute reliably across distributed worker pools with at-least-once delivery, strictly exactly-once state transitions, lease fencing, and zero dual-write persistence.

---

## Architectural Principles & Hard Constraints

1. **Zero Dual-Write**: Never introduce an external broker (Redis, RabbitMQ, Kafka) for queue dispatch. State transitions and queue claims are unified transactionally in PostgreSQL (or SQLite for embedded testing) via `FOR UPDATE SKIP LOCKED`.
2. **At-Least-Once Delivery with Monotonic Lease Fencing**: Workers claim tasks with an atomic `lease_token`. Late heartbeats or completions from expired workers MUST be rejected with `409 Conflict`.
3. **Declarative DAGs Only**: Do NOT implement Temporal-style non-deterministic code replay. Workflows are declarative DAGs validated using 3-color DFS cycle detection.
4. **Security Boundary**: Never execute arbitrary shell strings (`sh -c` or `cmd.exe /c`). Subprocess execution must use direct `exec.Command` with sanitized environment variables (database credentials and cloud tokens stripped) and OS process group limits.
5. **No Placeholders / Stubs**: Never write dummy implementations, mock mocks, or empty TODOs in production paths. Every feature must have complete error handling and transactional guarantees.

---

## Directory Structure Conventions

```text
cmd/jobengine/       # Single unified CLI and daemon entrypoint
internal/api/        # HTTP REST handlers, middleware, request validation
internal/store/      # Storage interface & SQL implementations (Postgres & SQLite)
internal/scheduler/  # Lease reaper, delayed sweeper, DAG propagator
internal/worker/     # Worker claim loop, heartbeat renewal, runner
internal/workflow/   # DAG cycle detection, validation, template interpolation
internal/telemetry/  # Prometheus metrics, Zap structured logging
pkg/client/          # Go client SDK for external services
migrations/          # SQL schema migration files
tests/integration/   # DB store integration tests
tests/chaos/         # Fault injection and crash recovery tests
docs/                # Architecture documents, ADRs, and master plan
```

---

## Development Commands

### Building
```bash
# Build the unified binary
go build -v -o bin/jobengine ./cmd/jobengine
```

### Testing
```bash
# Run unit tests with race detection (mandatory for all concurrency code)
go test -v -race ./internal/...

# Run integration tests (using SQLite embedded engine)
go test -v -race ./tests/integration/...

# Run chaos and failure-recovery tests
go test -v -race ./tests/chaos/...

# Run full test suite with coverage report
go test -v -race -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

### Linting & Formatting
```bash
# Format code
go fmt ./...

# Vet code
go vet ./...

# Run golangci-lint (if installed)
golangci-lint run ./...
```

---

## Database & Queue Conventions

1. **Storage Interface**: All store interactions must go through the `store.Store` Go interface in `internal/store/store.go`.
2. **Claiming**: Workers claim available tasks using `FOR UPDATE SKIP LOCKED`.
3. **Fencing**: Completion updates must condition on `WHERE id = $1 AND lease_token = $2 AND status = 'RUNNING'`.
4. **Tenant Isolation**: Every SQL query operating on user data must enforce `WHERE tenant_id = $1`.

---

## Sensitive & Generated Paths

- **Sensitive**:
  - Never commit `.env` files or API secrets.
  - Subprocess runner must scrub environment variables before spawning child processes.
- **Generated**:
  - `bin/` (compiled binaries)
  - `coverage.out` (test coverage reports)

---

## Forbidden Shortcuts

- ❌ NEVER commit `SELECT ... FOR UPDATE` without `SKIP LOCKED` for queue claims (causes row lock contention).
- ❌ NEVER allow transitions from terminal states (`COMPLETED`, `FAILED`, `CANCELLED`, `TIMED_OUT`).
- ❌ NEVER pass unsanitized user inputs to a shell interpreter.
- ❌ NEVER bypass the `lease_token` check during task completion or heartbeat renewal.
- ❌ NEVER invent external infrastructure requirements (Kafka, Redis, Kubernetes) when PostgreSQL satisfies the architectural contract.
