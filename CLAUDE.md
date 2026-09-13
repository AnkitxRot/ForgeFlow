# CLAUDE.md - Coding Agent Guidelines for ForgeFlow

## Repository Purpose
ForgeFlow (`github.com/AnkitxRot/ForgeFlow`) is a distributed, durable workflow and job execution engine written in Go. It enables client applications to submit atomic tasks and multi-step DAG workflows that execute reliably across distributed worker pools with at-least-once delivery, strictly exactly-once state transitions, lease fencing generations, and zero dual-write persistence.

---

## Architectural Principles & Hard Constraints

1. **Zero Dual-Write**: Never introduce an external broker (Redis, RabbitMQ, Kafka, NATS) for queue dispatch. State transitions and queue claims are unified transactionally in PostgreSQL (or SQLite for embedded testing) via `FOR UPDATE SKIP LOCKED`.
2. **At-Least-Once Delivery with Monotonic Fencing**: Workers claim tasks with an incremented `fencing_generation` and a unique `lease_token`. Late heartbeats or completions from expired workers MUST be rejected with `409 Conflict`.
3. **Declarative DAGs Only**: Do NOT implement Temporal-style non-deterministic code replay. Workflows are declarative DAGs validated using 3-color DFS cycle detection.
4. **Security Boundary**: Never execute arbitrary shell strings (`sh -c` or `cmd.exe /c`). Subprocess execution must use direct `exec.Command` with sanitized environment variables (database credentials and cloud tokens stripped) and OS process group limits.
5. **No Placeholders / Stubs**: Never write dummy implementations, mock mocks, or empty TODOs in production paths. Every feature must have complete error handling and transactional guarantees.

---

## Directory Structure Conventions

```text
cmd/forgeflow/       # Single unified CLI and daemon entrypoint
internal/api/        # HTTP REST handlers, middleware, request validation
internal/domain/     # Core domain entities, status enums, and state machine transitions
internal/store/      # Storage interface & SQL implementations (Postgres & SQLite)
internal/scheduler/  # Lease reaper, delayed sweeper, DAG propagator
internal/worker/     # Worker claim loop, heartbeat renewal, runner
internal/workflow/   # DAG cycle detection, validation, template interpolation
internal/telemetry/  # Prometheus metrics, structured logging
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
go build -v -o bin/forgeflow.exe ./cmd/forgeflow
```

### Testing
```bash
# Run unit tests with race detection (mandatory for all concurrency code)
go test -v -race ./internal/...

# Run pure-Go SQLite tests without CGO
CGO_ENABLED=0 go test -v ./internal/store/sqlite/...

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
```

---

## Database & Queue Conventions

1. **Storage Interface**: All store interactions must go through the `store.Store` Go interface in `internal/store/forgeflow_store.go`.
2. **Claiming**: Workers claim available tasks using `FOR UPDATE SKIP LOCKED` (PostgreSQL) or serialized immediate transactions (SQLite testing).
3. **Fencing**: Completion updates must condition on `WHERE id = $1 AND fencing_generation = $2 AND lease_token = $3 AND status = 'RUNNING'`.
4. **Tenant Isolation**: Every SQL query operating on user data must enforce `WHERE tenant_id = $1`.

---

## Development Requirements

- Go version: `1.27.1` (or Go 1.24+).
- Module path: `github.com/AnkitxRot/ForgeFlow`.
- SQLite driver: Pure-Go zero-CGO driver (`modernc.org/sqlite`).
- PostgreSQL driver: `github.com/jackc/pgx/v5`.

---

## Multi-Agent Development Rules

All coding agents working on this repository must adhere to:
1. **Inspect Before Modifying**: Never assume repository state; always run `git status`, verify existing files, and check compile/test status first.
2. **Make Bounded Changes**: Implement only the single assigned milestone or bounded task. Do not jump ahead to future phases.
3. **Preserve Unrelated Changes**: Do not reset, reformat, or alter code outside your immediate scope.
4. **Deterministic Verification**: Every implementation change must be accompanied by tests. Run `go test -race ./...`, `go vet ./...`, and `go build ./...` before declaring completion.
5. **No Speculative Refactoring**: Do not introduce unrequested generic abstractions, interfaces with only one implementation, or "util/helper" junk drawers.
6. **Provide Concrete Evidence**: Never mark criteria PROVEN based on assumptions or code inspection alone. Output actual test logs and command exit codes.

---

## Forbidden Shortcuts

- ❌ NEVER commit `SELECT ... FOR UPDATE` without `SKIP LOCKED` for queue claims (causes row lock contention).
- ❌ NEVER allow transitions from terminal states (`COMPLETED`, `FAILED`, `CANCELLED`, `TIMED_OUT`).
- ❌ NEVER pass unsanitized user inputs to a shell interpreter.
- ❌ NEVER bypass the `fencing_generation` or `lease_token` check during task completion or heartbeat renewal.
- ❌ NEVER invent external infrastructure requirements (Kafka, Redis, Kubernetes) when PostgreSQL satisfies the architectural contract.
