# ADR-003: Declarative Directed Acyclic Graph (DAG) Workflow Engine

- **Status**: Accepted
- **Date**: 2026-09-13
- **Deciders**: Principal Systems Architect

## Context

Workflow engines generally follow one of two paradigms:
1. **Code-as-Workflows (Temporal / Cadence style)**:
   - Workflows are written as imperative code functions (in Go, Java, TypeScript, etc.).
   - Orchestration relies on deterministic replay of event logs.
   - Any non-deterministic operation (system time, random numbers, thread races, updating dependency libraries, code edits) breaks replay determinism unless strict versioning markers and specialized runtime coroutines are used.
   - Cross-language workflows are difficult; authoring a workflow requires compiling and deploying specialized workflow worker processes.
2. **Declarative DAGs (Airflow / Argo / GitHub Actions style)**:
   - Workflows are defined declaratively as Directed Acyclic Graphs (JSON, YAML, or via fluent SDK builders that generate declarative DAG specs).
   - Each node is a distinct step with explicit input definitions, dependencies (`depends_on`), retry policies, timeouts, and handlers.
   - State transition is purely data-driven: when node dependencies resolve, the node transitions to `QUEUED`.
   - Inspection, auditing, visualization, and validation are immediate and transparent without executing code or replaying event histories.

## Decision

We adopt a **Declarative Directed Acyclic Graph (DAG)** workflow execution model.
- Workflows are specified as structured JSON/YAML DAG definitions containing:
  - Step identifiers and dependencies (`depends_on: ["step_a", "step_b"]`).
  - Step handler identity (e.g. `handler: "resize_image"` or `handler: "exec"`).
  - Explicit parameter bindings with JSONPath/Template evaluation (`{{ steps.step_a.output.file_url }}`).
  - Execution condition predicates (e.g. `when: "{{ steps.step_a.status == 'COMPLETED' }}"`).
  - Node-level retry policies, timeouts, and failure behaviors (`FAIL_WORKFLOW`, `CONTINUE`, `FALLBACK`).
- Workflow validation runs before submission:
  - Directed cycle detection using depth-first search (DFS) with three-color node marking (White/Gray/Black).
  - Validation of parameter references ensuring steps only reference ancestors.
  - Rejection of invalid DAGs at the API boundary with descriptive errors (`400 Bad Request`).

## Step Progression Engine
- When a workflow is created, all steps with zero dependencies (`in-degree == 0`) transition immediately to `QUEUED`.
- When a step completes, the state transition executes in an atomic transaction:
  1. Record step output in `workflow_steps`.
  2. Query immediate child steps whose prerequisites are now completely satisfied.
  3. Resolve input templates from completed ancestors.
  4. Atomically enqueue ready child steps into the `jobs` table.
  5. Check if all steps in the DAG have reached terminal state; if so, mark workflow `COMPLETED`.

## Consequences

### Positive
- **Determinism & Transparency**: No magic replay bugs. The status of a workflow is inspectable directly in SQL or API JSON without running a workflow interpreter.
- **Language Agnostic**: Any worker written in Go, Python, Node, Rust, or a raw CLI binary can execute a task.
- **Zero Drift**: Upgrading worker code does not invalidate in-flight workflows because execution is milestone-based rather than line-by-line code replay.
- **Resilience**: If the scheduler process restarts, no workflow state is lost or needs replay; unexecuted steps remain in their persisted state.

### Negative / Trade-offs
- Dynamic looping (e.g. `while condition do`) requires loop unrolling or recursive sub-workflow invocation rather than a native code `for` loop.

## Revisit Conditions
- If user demand strictly mandates arbitrary long-running imperative coroutines running across weeks with sleep timers embedded inside arbitrary multi-threaded user code.
