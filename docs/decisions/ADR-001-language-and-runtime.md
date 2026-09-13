# ADR-001: Selection of Primary Language and Runtime

- **Status**: Accepted
- **Date**: 2026-09-13
- **Deciders**: Principal Systems Architect

## Context

A distributed durable workflow and job execution platform requires:
1. High-throughput, low-latency networking and concurrent task dispatching.
2. Rock-solid concurrency primitives for scheduling loops, heartbeat monitors, and background sweepers without race conditions or memory leaks.
3. Single-binary cross-platform operational deployment (Linux, Windows, macOS) without complex external runtime dependencies or virtual environment management.
4. Fast compilation, strict static typing, and robust standard library support for HTTP, TLS, SQL, and OS process management.
5. First-class testing infrastructure with built-in race detection (`go test -race`).

Candidates evaluated:
- **Go (1.24+)**: Industry standard for cloud-native systems (Kubernetes, Docker, HashiCorp Nomad, CockroachDB, Temporal worker core). Native concurrency (goroutines, channels, `sync/atomic`, `context.Context`), single static binary output, predictable GC pause characteristics (<1ms), built-in race detector.
- **Rust**: High memory safety without GC, zero-cost abstractions. However: steep development and refactoring friction, significantly longer compilation times, lacks native installed runtime in the immediate development environment, and higher barrier for broad open-source community contributions.
- **Python (3.13)**: Rich scripting ecosystem. However: GIL limitations for multi-threaded CPU scheduling, higher memory footprint, runtime type failures without strict manual mypy tooling, fragile dependency distribution (`venv`, wheel binaries across OS platforms).

## Decision

We select **Go** as the primary implementation language for the API server, scheduler, worker engine, CLI, and client SDK.

## Consequences

### Positive
- **Single Artifact Deployment**: Scheduler, API, and Worker can run combined in a single binary or decoupled into discrete binary roles via CLI flags (`forgeflow server`, `forgeflow worker`, `forgeflow scheduler`).
- **Race Detection**: `go test -race` guarantees deterministic verification of concurrency bugs and memory synchronization during testing.
- **Predictable Resource Footprint**: Small baseline memory consumption (~20MB RSS) compared to JVM or Python runtimes.
- **Standard Library Strength**: Direct usage of `net/http`, `database/sql`, `os/exec`, `context` minimizes bloated third-party dependencies.

### Negative / Trade-offs
- Lack of generics-based monads/exceptions requires explicit, disciplined `if err != nil` error propagation.
- SQL queries and schema migrations require disciplined struct mapping and explicit scanning (mitigated using `pgx` and standard `database/sql`).

## Revisit Conditions
- If ultra-low-level kernel or sandbox virtualization features (e.g., custom Linux seccomp/eBPF JIT) require C/Rust bindings that Go cgo cannot cleanly support.
