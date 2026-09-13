# ADR-002: Persistence Engine and Queue Architecture

- **Status**: Accepted
- **Date**: 2026-09-13
- **Deciders**: Principal Systems Architect

## Context

A distributed workflow engine must store:
1. Workflow DAG topology and node dependencies.
2. Job execution records, state transitions, attempts, and audit logs.
3. The queue of runnable jobs ready for worker dispatch.

### The Dual-Write Hazard of External Brokers
Many naive queue systems combine an ACID relational database (for state/workflows) with an external queue broker (e.g., Redis, RabbitMQ, Kafka, or NATS).
This introduces the classic **Dual-Write Problem**:
- If Job state is committed to DB, but message publishing to Kafka/RabbitMQ fails or times out, the job is orphaned and never executed.
- If message is published to the broker first, but DB transaction rolls back or fails, a worker claims a ghost job with no matching DB record.
- Resolving this requires two-phase commit (2PC) or an outbox polling publisher pattern, which introduces additional latency, infrastructure failure points, and operational burden.

### Database-Backed Queues via `SKIP LOCKED`
Modern relational databases (specifically PostgreSQL) provide `SELECT ... FOR UPDATE SKIP LOCKED`.
- Workers can claim jobs concurrently without lock contention or deadlocks.
- Enqueueing a new workflow with 20 dependency steps and activating the root step occurs within a **single ACID transaction**.
- Job cancellation, timeouts, retries, and state transitions are 100% transactional and synchronous.

## Decision

1. We adopt **PostgreSQL** as the primary storage and queue persistence engine for production deployments.
2. We reject separate external message brokers (Redis, Kafka, RabbitMQ) for queue state.
3. We define an explicit storage interface (`Store`) in Go, supporting:
   - **PostgreSQL (`pgx`)**: The official production implementation with `FOR UPDATE SKIP LOCKED`, JSONB for dynamic step payloads/results, and partial indexes for hot queue queries.
   - **SQLite (WAL mode)**: An embedded implementation for local zero-dependency development, unit testing, and standalone single-node deployments.

## Queue Claiming Strategy
Workers claim available jobs via an atomic CTE query:
```sql
WITH candidate AS (
    SELECT id
    FROM jobs
    WHERE status = 'QUEUED'
      AND queue_name = ANY($1)
      AND run_at <= NOW()
    ORDER BY priority DESC, run_at ASC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $2
)
UPDATE jobs j
SET status = 'RUNNING',
    worker_id = $3,
    lease_token = gen_random_uuid(),
    lease_expires_at = NOW() + ($4 || ' seconds')::INTERVAL,
    attempt = attempt + 1,
    updated_at = NOW()
FROM candidate
WHERE j.id = candidate.id
RETURNING j.id, j.queue_name, j.workflow_id, j.workflow_step_id, j.priority, 
          j.payload, j.attempt, j.max_retries, j.lease_token, j.timeout_seconds;
```

## Consequences

### Positive
- **Zero Dual-Write Race**: Creating a workflow and placing its ready steps into the queue is 100% transactional.
- **Operational Simplicity**: Teams only need to operate, back up, and monitor a single PostgreSQL database instead of a database + message broker cluster.
- **Transactional Consistency**: If a worker completes step A, marking step A `COMPLETED` and queueing downstream steps B and C is atomic.
- **Predictable Local DX**: Developers can run unit and integration tests instantly using embedded SQLite without running Docker containers.

### Negative / Trade-offs
- Very high throughput (>50,000 job dispatches/sec on a single queue) can generate WAL write volume and table bloat in PostgreSQL if MVCC autovacuum is not properly tuned.
- Mitigated by:
  1. Indexing only hot uncompleted rows (`WHERE status IN ('QUEUED', 'RUNNING')`).
  2. Archiving or partitioning completed job records into cold history tables or daily partitions.

## Revisit Conditions
- If single-cluster throughput requirements exceed 30,000 dispatches/sec continuously, requiring sharding or an ephemeral memory queue with transactional WAL.
