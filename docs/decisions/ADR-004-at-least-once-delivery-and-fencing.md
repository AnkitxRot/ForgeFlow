# ADR-004: Delivery Semantics, Lease Fencing, and Exactly-Once State Transitions

- **Status**: Accepted
- **Date**: 2026-09-13
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
2. **Exactly-Once State Transitions via Fencing Tokens**:
   - The platform enforces **strictly exactly-once state transitions** at the database layer.
   - Every time a job is claimed or reassigned, the database atomically increments `attempt` and generates a cryptographically secure monotonic `lease_token` (UUIDv4).
   - All worker mutations (heartbeat renewals, completions, failures) **must** present the `lease_token`.
3. **Atomic Conditional Updates**:
   Completion is executed as a conditional update:
   ```sql
   UPDATE jobs
   SET status = 'COMPLETED',
       result = $1,
       completed_at = NOW(),
       updated_at = NOW()
   WHERE id = $2
     AND lease_token = $3
     AND status = 'RUNNING';
   ```
   If the lease has expired and been reclaimed by another worker (or cancelled by an administrator), the `lease_token` no longer matches.
   The database affects `0` rows.
   The API responds with `409 Conflict: Lease Revoked or Expired (ERR_LEASE_LOST)`.
4. **Worker Lease Revocation Protocol**:
   When a worker receives `409 Conflict` on a heartbeat or completion:
   - The worker must immediately cancel its local execution context (`context.CancelFunc()`).
   - The worker must discard any local buffers or output artifacts.
   - The event is recorded in the worker metrics and structured logs as a `ZOMBIE_EXECUTION_SUPPRESSED`.
5. **Submission Deduplication / Idempotency Keys**:
   - Job and workflow submissions accept an optional `idempotency_key`.
   - A unique constraint `UNIQUE (tenant_id, idempotency_key)` prevents accidental duplicate submissions from retried HTTP POST requests. Duplicate requests return the original submitted entity.

## Consequences

### Positive
- **No Split-Brain Inconsistency**: A late or revived zombie worker can never corrupt job state or trigger duplicate downstream workflow execution.
- **Formally Verifiable**: The correctness of the fencing token guarantee can be verified with automated chaos and concurrent race tests.
- **Clear Developer Contract**: Application authors know their job handlers must be idempotent or use deduplication keys if interacting with non-transactional external systems.

### Negative / Trade-offs
- If a worker completes an external non-idempotent side effect (e.g. sending a credit card charge via an external payment gateway) and then crashes before acknowledging, a subsequent retry will execute the handler again.
- Handlers performing external mutations must use idempotency tokens provided in the job context (`job.IdempotencyKey` / `job.Attempt`).

## Revisit Conditions
- None. Fencing tokens and conditional mutation are fundamental distributed systems invariants.
