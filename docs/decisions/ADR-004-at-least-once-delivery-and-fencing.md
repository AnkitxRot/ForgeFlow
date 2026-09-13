# ADR-004: Delivery Semantics, Lease Fencing Generations, and Exactly-Once State Transitions

- **Status**: Accepted
- **Date**: 2026-09-13 (Amended)
- **Deciders**: Principal Systems Architect

## Context

In any distributed asynchronous system operating over an unreliable network:
1. Messages can be delayed, reordered, or duplicated.
2. Workers may experience arbitrary stop-the-world garbage collection pauses, CPU throttling, or network partitions (the "split-brain" worker problem).
3. If Worker A believes it owns Job 1, but its lease expires due to a network delay, the Scheduler legitimately reassigns Job 1 to Worker B.
4. If Worker A later completes its computation and attempts to commit results or trigger downstream workflow steps, a race condition occurs: both Worker A and Worker B might commit conflicting results or run downstream steps twice.

Claiming "strictly exactly-once physical execution" in a distributed system with worker crashes is mathematically impossible (violates the Two-Generals Problem and FLP Impossibility). The external side effect executed by a worker cannot be rolled back if the worker dies immediately after performing it.

## Decision

1. **At-Least-Once Delivery**: The platform guarantees **at-least-once task delivery and execution**. Tasks may be dispatched to a worker more than once if a previous worker crashes or fails to renew its lease before the deadline.
2. **Three Distinct Execution Attributes**:
   - **`attempt` (INT)**: Logical execution attempt / retry counter ($1, 2, 3 \dots$). Used to enforce retry limits (`max_retries`), calculate exponential backoffs, and track attempt history in audit logs.
   - **`fencing_generation` (BIGINT)**: Monotonically increasing ownership epoch ($1, 2, 3 \dots$). Advanced every time a job is claimed or re-queued (`fencing_generation = fencing_generation + 1`). Any worker carrying an older generation is mathematically superseded.
   - **`lease_token` (UUIDv4 string)**: Cryptographically secure, unguessable lease epoch token. Serves as a private ownership credential for the lease session, preventing unauthorized or guessing workers from interacting with the task.
3. **Atomic Conditional Updates**:
   Completion and heartbeat mutations are executed as strict conditional updates:
   ```sql
   UPDATE jobs
   SET status = 'COMPLETED',
       result = $1,
       completed_at = NOW(),
       updated_at = NOW()
   WHERE id = $2
     AND fencing_generation = $3
     AND lease_token = $4
     AND status = 'RUNNING';
   ```
   If the lease has expired and been reclaimed by another worker (which advanced `fencing_generation` and generated a new `lease_token`), the condition matches **0 rows**.
   The database rejects the mutation, and the store returns `ErrLeaseLost` (HTTP `409 Conflict`).
4. **Worker Lease Revocation Protocol**:
   When a worker receives `409 Conflict` on a heartbeat or completion:
   - The worker must immediately cancel its local execution context (`context.CancelFunc()`).
   - The worker terminates the child subprocess group immediately.
   - The worker discards any local buffers or output artifacts.
   - The event is recorded in worker metrics as a `ZOMBIE_EXECUTION_SUPPRESSED`.
5. **Submission Deduplication / Idempotency Keys**:
   - Job and workflow submissions accept an optional `idempotency_key`.
   - A unique constraint `UNIQUE (tenant_id, idempotency_key)` prevents accidental duplicate submissions from retried HTTP POST requests. Duplicate submissions return HTTP 409 Conflict (`idempotency key conflict`), guaranteeing at-most-once job creation.

## Consequences

### Positive
- **No Split-Brain Inconsistency**: A late or revived zombie worker can never corrupt job state or trigger duplicate downstream workflow execution.
- **Formally Verifiable**: The correctness of the fencing generation guarantee can be verified with automated chaos and concurrent race tests.
- **Clear Separation of Concerns**: Monotonic generation protects sequence ordering; UUID lease token protects authorization secrecy; attempt counter manages retry exhaustion.

### Negative / Trade-offs
- External mutations performed before worker crash must be idempotent or use `job.IdempotencyKey`.

## Revisit Conditions
- None. Fencing generations and conditional atomic mutations are fundamental distributed systems invariants.
