# JobEngine

> **Distributed Durable Workflow & Job Execution Platform**  
> Designed for failure, concurrency, recovery, and operational correctness.

---

## Overview

JobEngine is an open-source, high-integrity distributed job and workflow engine. It provides reliable execution of atomic jobs and multi-step directed acyclic graph (DAG) workflows across a pool of distributed workers.

Key architectural pillars:
- **Zero Dual-Write**: Unifies queue dispatch, workflow state, and execution history in an ACID relational persistence engine using PostgreSQL `FOR UPDATE SKIP LOCKED`.
- **At-Least-Once Delivery & Monotonic Fencing**: Late or partitioned zombie workers cannot corrupt state or trigger duplicate downstream executions.
- **Declarative DAG Workflows**: Deterministic, inspectable multi-step workflows with static cycle detection and parameter piping.
- **Operational Simplicity**: Single-binary Go architecture with embedded SQLite mode for zero-dependency local testing.

---

## Documentation & Architecture

Complete engineering specifications, architectural decisions, and planning roadmaps are located in `docs/`:

- **[Master Engineering Plan](docs/planning/master-plan.md)**: 31-section comprehensive specification of system architecture, data models, state machines, API, CLI, and test strategy.
- **[System Architecture](docs/architecture/system-architecture.md)**: Topology diagrams, component responsibilities, sequence flows, and failure recovery mechanics.
- **Architectural Decision Records (ADRs)**:
  - [ADR-001: Language & Runtime (Go)](docs/decisions/ADR-001-language-and-runtime.md)
  - [ADR-002: Persistence & Queue Engine (PostgreSQL SKIP LOCKED)](docs/decisions/ADR-002-persistence-and-queue-engine.md)
  - [ADR-003: Workflow Model (Declarative DAGs)](docs/decisions/ADR-003-workflow-dag-model.md)
  - [ADR-004: Delivery Semantics & Lease Fencing](docs/decisions/ADR-004-at-least-once-delivery-and-fencing.md)
  - [ADR-005: Worker Security & Subprocess Isolation](docs/decisions/ADR-005-worker-security-and-isolation.md)

---

## Agent & Contributor Guide

For instructions on coding standards, forbidden shortcuts, test commands, and build commands, consult **[CLAUDE.md](CLAUDE.md)**.
