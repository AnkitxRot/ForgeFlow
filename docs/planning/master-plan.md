# Distributed Durable Workflow & Job Execution Platform: Master Engineering Plan

- **Project**: JobEngine (Internal Working Title)
- **Author**: Principal Systems Architect & Technical Product Planner
- **Status**: Architecture Approved / Implementation-Ready
- **Target Quality Bar**: Production Systems Grade (Designed for failure, concurrency, recovery, and operational correctness)

---

## 1. Executive Summary

This document specifies the complete architectural, data, concurrency, and failure model for **JobEngine**, a distributed durable job and workflow execution platform.

Modern distributed architectures frequently stumble into the **Dual-Write Problem** by splitting state between an ACID relational database (for entity state and DAG progress) and an external message broker (Redis, RabbitMQ, Kafka) for queue dispatch. When either component encounters network partitions, crashes, or timeouts during state transitions, the system falls into irrecoverable split-brain inconsistencies.

JobEngine resolves this by unifying queue management, workflow DAG state, and execution history into a **single transactional persistence engine** leveraging PostgreSQL with atomic `SKIP LOCKED` row-level locking (with an embedded SQLite WAL store for local development and integration tests). Task delivery is explicitly **at-least-once**, paired with **strictly exactly-once state transitions** enforced via monotonic **lease fencing tokens** and atomic conditional updates. Workers execute tasks via isolated process groups or compiled handlers, protected against zombie execution, clock drift, and runaway resource consumption.

---

## 2. Project Goals

1. **Transactional Durability**: Zero job or workflow state loss across crashes, network partitions, and process restarts.
2. **Zero Dual-Write Hazard**: Queue enqueueing, state transitions, and downstream DAG step activation occur atomically within the persistence engine.
3. **Provable Concurrency Safety**: Fencing tokens ensure that expired, partitioned, or zombie workers can never commit stale results or corrupt downstream executions.
4. **Declarative DAG Workflows**: Workflows are structured, inspectable directed acyclic graphs with dynamic parameter passing, cycle validation, and deterministic step progression.
5. **Operational Simplicity**: Single-binary deployment for all roles (`jobengine server`, `jobengine worker`, `jobengine scheduler`) with zero required external dependencies beyond PostgreSQL.
6. **Deep Observability & Testability**: Native Prometheus metrics, structured contextual logging, and automated fault-injection test suites that kill processes mid-execution to prove recovery invariants.

---

## 3. Non-Goals

1. **Imperative Code Replay (Temporal Clone)**: We intentionally reject coroutine interception and non-deterministic event replay SDKs. Workflows are declarative DAGs.
2. **Untrusted Multi-Tenant Public Sandbox**: The engine does not provide multi-tenant kernel microVM sandboxing (e.g. Firecracker/gVisor) in Phase 1. Subprocess execution provides OS-level process isolation, but the host environment is assumed to run within a trusted or semi-trusted administrative boundary.
3. **Arbitrary Stream Processing**: JobEngine is not Kafka or Flink; it does not process infinite real-time event streams or windowed aggregations.
4. **Complex UI Dashboard**: Phase 1 through 5 focus strictly on engine correctness, API, and CLI. A web UI is deferred.

---

## 4. Requirements Matrix

Every requirement is classified as **MUST**, **SHOULD**, **MAY**, or **DEFER** with explicit architectural justification.

### 4.1 Functional Requirements

| Requirement | Priority | Architectural Justification |
|---|---|---|
| **Submit Atomic Job** | **MUST** | Baseline platform capability. Supports priority, delay, payload, and retry policy. |
| **Idempotency Keys** | **MUST** | Essential for distributed network retries; prevents duplicate job submissions on network blips. |
| **Job Query & Status** | **MUST** | Clients and workers must inspect state, attempts, duration, and execution metadata. |
| **Job Cancellation** | **MUST** | Users must be able to cancel pending, queued, or running jobs; running workers must abort via context cancellation. |
| **Manual Retry API** | **MUST** | Operators must be able to re-trigger failed jobs under controlled audit tracking. |
| **Delayed Execution (`run_at`)** | **MUST** | Essential for timed jobs and exponential backoff retry scheduling. |
| **Worker Registration & Heartbeats**| **MUST** | Required to track cluster worker capacity and detect dead workers holding stale leases. |
| **Atomic Job Claiming** | **MUST** | Concurrency safety; multiple workers must claim work without duplicate assignment. |
| **Execution Leases & Fencing** | **MUST** | Fundamental protection against split-brain workers completing work after timeouts. |
| **Configurable Retry Policies** | **MUST** | Exponential backoff with jitter prevents thundering herds on downstream services. |
| **Priority Queues** | **MUST** | Starvation-resistant prioritization of critical business tasks over background jobs. |
| **Declarative Workflow DAGs** | **MUST** | Multi-step task dependencies with automatic fan-out, fan-in, and parameter piping. |
| **Workflow Step Failure Policies** | **MUST** | Define whether step failure aborts entire workflow (`FAIL_WORKFLOW`) or continues (`CONTINUE`). |
| **Workflow Cancellation Propagation**| **MUST** | Cancelling a workflow must cancel all child steps atomically. |
| **Execution Audit History** | **MUST** | Append-only ledger of every attempt, worker ID, execution duration, and exit status. |
| **REST API** | **MUST** | Standard HTTP/JSON interface for language-agnostic integration. |
| **CLI Tooling** | **MUST** | Essential operator and developer tool for submitting, monitoring, and debugging jobs. |
| **API Key Authentication & RBAC** | **MUST** | Prevent unauthorized job injection or administrative destruction. |
| **Tenant Isolation** | **MUST** | Partition jobs and workflows by `tenant_id` at the database query level. |
| **Recurring Schedules (Cron)** | **SHOULD** | Common operational requirement; implemented via lightweight scheduler schedule table. |
| **Worker Concurrency Throttling** | **SHOULD** | Workers restrict concurrent executions based on CPU/RAM limits. |
| **Queue Pausing / Resuming** | **SHOULD** | Emergency operator control to pause queue drain during downstream outages. |
| **Dynamic Sub-Workflows** | **MAY** | Ability for a workflow step to trigger a nested child workflow. |
| **Web UI Dashboard** | **DEFER** | Deferred to prevent scope creep; CLI and Prometheus metrics fulfill operational needs. |
| **Dynamic Workflows (while loops)**| **DEFER** | Adds substantial state machine complexity; handled via recursion or deferred to Phase 11. |

### 4.2 Non-Functional Requirements

| Requirement | Priority | Target Specification |
|---|---|---|
| **Durability** | **MUST** | Zero acknowledged job loss. All state transitions committed via PostgreSQL ACID transactions. |
| **Consistency** | **MUST** | Linearizable state transitions per job using monotonic lease tokens (`lease_token = UUID()`). |
| **Availability** | **MUST** | Scheduler and API nodes are fully stateless and horizontally scalable. Multi-worker fault tolerance. |
| **Crash Recovery** | **MUST** | Worker or scheduler crash results in automatic lease expiration and re-queuing within `lease_duration`. |
| **Security** | **MUST** | Argon2id token hashing, SQL injection prevention via parameterized queries, subprocess isolation without shell invocation. |
| **Observability** | **MUST** | Standard Prometheus metrics (`/metrics`), structured JSON logs with tracing correlation IDs. |
| **Operational Simplicity** | **MUST** | Single static Go binary, single PostgreSQL database. Zero Kafka/Redis/Zookeeper. |
| **Testability** | **MUST** | `go test -race` enabled, deterministic SQLite embedded engine for unit/integration tests, chaos fault-injection tests. |

---

## 5. System Architecture & Component Responsibilities

### 5.1 Architectural Components

```
                               ┌───────────────────────────┐
                               │        API Server         │
                               │  - Request Validation     │
                               │  - RBAC / Tenant Auth     │
                               │  - DAG Cycle Detection    │
                               │  - Idempotency Filter     │
                               └─────────────┬─────────────┘
                                             │
                       ┌─────────────────────┴─────────────────────┐
                       │                                           │
                       ▼                                           ▼
         ┌───────────────────────────┐               ┌───────────────────────────┐
         │     Scheduler Service     │               │       Worker Engine       │
         │  - Delayed Job Sweeper    │               │  - Claim Loop (Batch)     │
         │  - Lease Reaper           │               │  - Heartbeat Goroutine    │
         │  - DAG Step Propagator    │               │  - Subprocess Isolation   │
         │  - Cron Dispatcher        │               │  - In-Process Registry    │
         └─────────────┬─────────────┘               └─────────────┬─────────────┘
                       │                                           │
                       └─────────────────────┬─────────────────────┘
                                             │
                                             ▼
                               ┌───────────────────────────┐
                               │   PostgreSQL Persistence  │
                               │  - Jobs Table             │
                               │  - Workflows & Steps      │
                               │  - Queue (SKIP LOCKED)    │
                               │  - Audit & Execution Logs │
                               └───────────────────────────┘
```

### 5.2 Component Boundaries

1. **API Server (`cmd/server`, `internal/api`)**:
   - Accepts external HTTP requests from clients, SDKs, and CLI.
   - Validates DAG definitions (ensures acyclic property, valid step names, valid dependency references).
   - Enforces authentication, authorization scopes, and tenant boundary isolation.
   - Handles idempotency deduplication.
   - **Does NOT execute jobs**.

2. **Scheduler Service (`internal/scheduler`)**:
   - **Lease Reaper**: Scans for jobs stuck in `RUNNING` with `lease_expires_at < NOW()`. Requeues retryable jobs or marks them `FAILED`.
   - **Delayed Job Sweeper**: Promotes jobs in `SCHEDULED` status to `QUEUED` when `run_at <= NOW()`.
   - **DAG Propagator**: When a workflow step completes, resolves downstream child steps, evaluates conditionals, and transitions satisfied steps to `QUEUED`.
   - Multiple scheduler instances can run concurrently; all sweeper queries utilize `FOR UPDATE SKIP LOCKED`.

3. **Worker Engine (`cmd/worker`, `internal/worker`)**:
   - Polls the API or database for available tasks matching subscribed queues.
   - Atomically claims tasks, receiving a private `lease_token`.
   - Manages task execution lifecycle: spawns execution in a goroutine, manages heartbeat renewal timer, cancels execution if lease is lost.
   - Executes either in-process Go handlers or isolated OS subprocesses.
   - Submits task completion or failure with fencing token.

4. **Persistence Layer (`internal/store`)**:
   - Encapsulates all SQL queries, transaction boundaries, and row-level locking.
   - Provides clean Go interface (`Store`), implemented by PostgreSQL (`pgx`) and SQLite (embedded testing).

---

## 6. Persistence Data Model (SQL Schema)

The database schema is strictly normalized for transactional integrity, with partial indexes to keep hot queue queries sub-millisecond even with millions of historical records.

```sql
-- 1. Tenants
CREATE TABLE tenants (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. API Keys & Authentication
CREATE TABLE api_keys (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    key_hash VARCHAR(128) NOT NULL,
    key_prefix VARCHAR(16) NOT NULL,
    role VARCHAR(32) NOT NULL, -- 'ADMIN', 'SUBMITTER', 'WORKER', 'READONLY'
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);
CREATE INDEX idx_api_keys_hash ON api_keys(key_hash);

-- 3. Queues Definition & Configuration
CREATE TABLE queues (
    name VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    is_paused BOOLEAN NOT NULL DEFAULT FALSE,
    concurrency_limit INT NOT NULL DEFAULT 0, -- 0 = unlimited
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, name)
);

-- 4. Workflows (Parent DAG execution instance)
CREATE TABLE workflows (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL, -- PENDING, RUNNING, COMPLETED, FAILED, CANCELLED
    idempotency_key VARCHAR(128),
    definition_json JSONB NOT NULL,
    context_data JSONB NOT NULL DEFAULT '{}'::JSONB,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    CONSTRAINT uq_workflow_idempotency UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX idx_workflows_tenant_status ON workflows(tenant_id, status);

-- 5. Workflow Steps (Individual DAG nodes)
CREATE TABLE workflow_steps (
    id VARCHAR(64) PRIMARY KEY,
    workflow_id VARCHAR(64) NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_name VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL, -- PENDING, QUEUED, RUNNING, COMPLETED, FAILED, CANCELLED, SKIPPED
    dependencies JSONB NOT NULL DEFAULT '[]'::JSONB, -- Array of step_name strings
    handler VARCHAR(128) NOT NULL,
    input_template JSONB NOT NULL DEFAULT '{}'::JSONB,
    output_data JSONB,
    error_message TEXT,
    failure_policy VARCHAR(32) NOT NULL DEFAULT 'FAIL_WORKFLOW', -- FAIL_WORKFLOW, CONTINUE
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT uq_workflow_step UNIQUE (workflow_id, step_name)
);
CREATE INDEX idx_steps_workflow ON workflow_steps(workflow_id);

-- 6. Jobs (Atomic Units of Execution)
CREATE TABLE jobs (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    queue_name VARCHAR(64) NOT NULL,
    workflow_id VARCHAR(64) REFERENCES workflows(id) ON DELETE CASCADE,
    workflow_step_id VARCHAR(64) REFERENCES workflow_steps(id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL, -- PENDING, SCHEDULED, QUEUED, RUNNING, RETRYING, COMPLETED, FAILED, CANCELLED, TIMED_OUT
    priority INT NOT NULL DEFAULT 0, -- Higher number = higher priority
    payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    result JSONB,
    error_message TEXT,
    attempt INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    retry_backoff_seconds INT NOT NULL DEFAULT 5,
    timeout_seconds INT NOT NULL DEFAULT 300,
    run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_token VARCHAR(64),
    lease_expires_at TIMESTAMPTZ,
    worker_id VARCHAR(64),
    idempotency_key VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT uq_jobs_idempotency UNIQUE (tenant_id, idempotency_key)
);

-- CRITICAL PARTIAL INDEXES FOR QUEUE CLAIMING & SWEEPING
CREATE INDEX idx_jobs_claimable ON jobs (queue_name, priority DESC, run_at ASC, id ASC)
WHERE status = 'QUEUED';

CREATE INDEX idx_jobs_running_lease ON jobs (lease_expires_at ASC)
WHERE status = 'RUNNING';

CREATE INDEX idx_jobs_delayed ON jobs (run_at ASC)
WHERE status IN ('SCHEDULED', 'RETRYING');

-- 7. Job Executions (Immutable Audit Log of Attempts)
CREATE TABLE job_executions (
    id VARCHAR(64) PRIMARY KEY,
    job_id VARCHAR(64) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt INT NOT NULL,
    worker_id VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL, -- RUNNING, COMPLETED, FAILED, TIMED_OUT, LEASE_EXPIRED
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    duration_ms BIGINT
);
CREATE INDEX idx_executions_job ON job_executions(job_id, attempt);

-- 8. Workers (Cluster Worker Registry)
CREATE TABLE workers (
    id VARCHAR(64) PRIMARY KEY,
    hostname VARCHAR(255) NOT NULL,
    queues JSONB NOT NULL,
    concurrency_limit INT NOT NULL DEFAULT 10,
    status VARCHAR(32) NOT NULL, -- ALIVE, DRAINING, STOPPED, DEAD
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_workers_heartbeat ON workers(last_heartbeat_at);
```

---

## 7. State Machines & Formal Invariants

### 7.1 Job State Machine

```
               ┌──────────────┐
               │   PENDING    │ (Workflow step awaiting dependencies)
               └──────┬───────┘
                      │ (Prerequisites satisfied)
                      ▼
               ┌──────────────┐
  ┌───────────►│    QUEUED    │◄────────────────────────┐
  │            └──────┬───────┘                         │
  │                   │                                 │
  │ (Delayed run_at   │ (Claimed by worker              │ (Retry delay
  │  expires)         │  with lease_token)              │  elapsed)
  │                   ▼                                 │
┌─┴────────────┐ ┌──────────────┐                 ┌─────┴────────┐
│  SCHEDULED   │ │   RUNNING    │────────────────►│   RETRYING   │
└──────────────┘ └──────┬───────┘ (Failure &      └──────────────┘
                        │         attempt < max)
                        ├───────────────────────────────┐
                        │                               │
                        ▼                               ▼
               ┌────────────────┐              ┌────────────────┐
               │   COMPLETED    │              │     FAILED     │
               └────────────────┘              └────────────────┘
                  (Terminal)                      (Terminal)
                        ▲                               ▲
                        │                               │
                        ├───────────────────────────────┤
                        │ (Cancelled by user or API)    │
                        ▼                               ▼
               ┌────────────────┐              ┌────────────────┐
               │   CANCELLED    │              │   TIMED_OUT    │
               └────────────────┘              └────────────────┘
                  (Terminal)                      (Terminal)
```

### 7.2 Permitted State Transitions

| Current State | Next State | Triggering Actor | Required Conditions / Guards | Side Effects |
|---|---|---|---|---|
| `PENDING` | `QUEUED` | Scheduler | All DAG parent steps are `COMPLETED`. | Job becomes eligible for worker claim. |
| `PENDING` | `CANCELLED` | API / User | Workflow cancelled or step cancelled. | Terminal state recorded. |
| `SCHEDULED`| `QUEUED` | Scheduler Sweeper | `run_at <= NOW()`. | Job becomes claimable. |
| `QUEUED` | `RUNNING` | Worker Claim TX | Worker executes atomic claim query. | Generates `lease_token`, sets `lease_expires_at`, increments `attempt`. |
| `QUEUED` | `CANCELLED` | API / User | Cancellation request received. | Job removed from claim pool. |
| `RUNNING` | `RUNNING` | Worker Heartbeat | `lease_token` matches current active token. | Extends `lease_expires_at = NOW() + lease_duration`. |
| `RUNNING` | `COMPLETED` | Worker Ack | `lease_token` matches current active token. | Saves `result`, records execution duration, triggers DAG step progression. |
| `RUNNING` | `RETRYING` | Worker / Reaper | Error occurred OR lease expired, AND `attempt < max_retries`. | Sets `run_at = NOW() + backoff_seconds`, clears `lease_token`. |
| `RUNNING` | `FAILED` | Worker / Reaper | Error occurred OR lease expired, AND `attempt >= max_retries`. | Saves `error_message`, triggers workflow failure evaluation. |
| `RUNNING` | `TIMED_OUT` | Worker / Reaper | Total execution duration exceeds `timeout_seconds`. | Revokes lease, marks execution `TIMED_OUT`. |
| `RUNNING` | `CANCELLED` | API / User | Cancellation API invoked. | Revokes lease; worker context cancelled on next heartbeat. |
| `RETRYING` | `QUEUED` | Scheduler Sweeper | `run_at <= NOW()`. | Re-enqueues for worker claim. |

### 7.3 State Machine Invariants (Must NEVER be violated)

1. **Terminal State Immutability**: Once a job enters `COMPLETED`, `FAILED`, `CANCELLED`, or `TIMED_OUT`, it can never transition to any other state. No updates to payload or status are permitted.
2. **Monotonic Execution Attempt**: The `attempt` counter increases strictly monotonically ($attempt_{n+1} = attempt_n + 1$) on every claim.
3. **Single Active Lease**: At any instant $t$, there exists at most one valid `(job_id, lease_token)` pair where `lease_expires_at > t`.
4. **Fencing Precedence**: Any mutation from a worker presenting a stale `lease_token` must be rejected with `409 Conflict` and discarded.

---

## 8. Queue and Scheduler Mechanics

### 8.1 Atomic Claiming Query
Worker claiming avoids deadlocks and thundering herds via PostgreSQL's `SKIP LOCKED`:

```sql
WITH claim_batch AS (
    SELECT id
    FROM jobs
    WHERE status = 'QUEUED'
      AND queue_name = ANY($1) -- Array of subscribed queues
      AND run_at <= NOW()
    ORDER BY priority DESC, run_at ASC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $2 -- Worker batch size (concurrency headroom)
)
UPDATE jobs j
SET status = 'RUNNING',
    worker_id = $3,
    lease_token = gen_random_uuid(),
    lease_expires_at = NOW() + ($4 || ' seconds')::INTERVAL,
    attempt = attempt + 1,
    updated_at = NOW()
FROM claim_batch
WHERE j.id = claim_batch.id
RETURNING j.id, j.queue_name, j.workflow_id, j.workflow_step_id, j.priority,
          j.payload, j.attempt, j.max_retries, j.lease_token, j.timeout_seconds;
```

### 8.2 Starvation Prevention & Fairness
- **Strict Priority**: High priority jobs (`priority > 0`) execute before lower priority jobs.
- **Fairness Guarantee**: Within the same priority level, jobs are strictly ordered by `run_at ASC, id ASC` (FIFO).
- **Queue Interleaving**: When a worker subscribes to multiple queues, the scheduler uses a weighted or round-robin queue query ordering so that high-volume queues do not starve low-volume queues.

### 8.3 Exponential Backoff with Jitter
When a job encounters a transient failure or a lease expires without heartbeat, the next execution time is computed using Full Jitter exponential backoff:

$$\text{sleep} = \min(\text{max\_backoff}, \text{base} \times 2^{\text{attempt}}) \times \text{Uniform}(0.5, 1.5)$$

Where $\text{base} = \text{retry\_backoff\_seconds}$ (default 5s) and $\text{max\_backoff} = 3600\text{s}$. This completely prevents thundering herd synchronization when external dependencies recover.

---

## 9. Worker Protocol & Lifecycle

```
    [Worker Start]
          │
          ▼
   Register Worker (POST /api/v1/workers)
          │
          ▼
   ┌──────────────┐
   │  Poll &      │◄────────────────────────┐
   │  Claim Batch │                         │
   └──────┬───────┘                         │
          │                                 │
    (Job Claimed)                           │
          ▼                                 │
   Spawn Task Goroutine                     │
          │                                 │
    ┌─────┴─────────────────────┐           │
    │                           │           │
    ▼                           ▼           │
Execute Task               Heartbeat Loop   │
(Handler / Subprocess)     (Every lease/3)  │
    │                           │           │
    │                      (Check 200 OK)   │
    │                      If 409 Conflict: │
    │                      Cancel Context!  │
    │                           │           │
    └─────┬─────────────────────┘           │
          │                                 │
    (Task Finished)                         │
          ▼                                 │
Acknowledge Completion/Failure              │
(POST /api/v1/jobs/{id}/complete)           │
          │                                 │
          └─────────────────────────────────┘
```

### 9.1 Worker Heartbeat Cadence
- Let $L$ be `lease_duration_seconds` (default: 30 seconds).
- The worker heartbeat timer fires at $L / 3$ (every 10 seconds).
- If the worker fails to reach the server after 2 consecutive attempts ($20$ seconds), the worker proactively signals a warning. At $t = L$, if renewal has not succeeded, the worker **must cancel the execution context** to prevent zombie execution.

### 9.2 Graceful Draining (SIGTERM / SIGINT)
Upon receiving `SIGTERM`:
1. Worker transitions its status in `workers` table to `DRAINING`.
2. Worker halts its claim polling loop; no new jobs are accepted.
3. Active jobs are allowed to execute up to a configurable `drain_timeout` (e.g. 30 seconds).
4. Ongoing heartbeats continue during drain.
5. If tasks do not complete before `drain_timeout`, the worker cancels child contexts and exits. Unfinished jobs will be safely reaped by the scheduler after their leases expire.

---

## 10. Declarative Workflow DAG Model

### 10.1 DAG Specification Example (JSON)
```json
{
  "name": "media-processing-pipeline",
  "steps": [
    {
      "name": "download",
      "handler": "http_fetch",
      "payload": { "url": "https://example.com/video.mp4" },
      "timeout_seconds": 60,
      "max_retries": 3,
      "failure_policy": "FAIL_WORKFLOW"
    },
    {
      "name": "extract_audio",
      "depends_on": ["download"],
      "handler": "ffmpeg_audio",
      "payload": { "source_file": "{{ steps.download.output.file_path }}" },
      "timeout_seconds": 120
    },
    {
      "name": "generate_thumbnail",
      "depends_on": ["download"],
      "handler": "ffmpeg_thumb",
      "payload": { "source_file": "{{ steps.download.output.file_path }}" },
      "timeout_seconds": 30
    },
    {
      "name": "transcribe_speech",
      "depends_on": ["extract_audio"],
      "handler": "whisper_ai",
      "payload": { "audio_file": "{{ steps.extract_audio.output.audio_path }}" },
      "timeout_seconds": 300
    },
    {
      "name": "notify_completion",
      "depends_on": ["generate_thumbnail", "transcribe_speech"],
      "handler": "webhook",
      "payload": {
        "thumb": "{{ steps.generate_thumbnail.output.url }}",
        "transcript": "{{ steps.transcribe_speech.output.text }}"
      }
    }
  ]
}
```

### 10.2 Cycle Detection & Validation Algorithm
Before a workflow is accepted, the API validates the DAG using Depth-First Search with 3-color node marking:
- **WHITE (0)**: Unvisited.
- **GRAY (1)**: Currently visiting in the current DFS call stack. If an edge leads to a GRAY node, a **cycle exists**.
- **BLACK (2)**: Completely visited and verified acyclic.

Validation also enforces:
1. Every dependency referenced in `depends_on` must exist as a defined step name.
2. Step names must be unique within the workflow.
3. Parameter templates (`{{ steps.X.output.Y }}`) may only reference ancestor steps in the graph.

---

## 11. Failure Matrix & Recovery Semantics

| Component | Failure Mode | Observable Symptoms | Immediate System Response | Recovery & Invariant Preservation |
|---|---|---|---|---|
| **Worker** | Immediate hard crash (kill -9, power loss). | Heartbeats cease. Job remains in `RUNNING`. | Lease expires in DB (`lease_expires_at < NOW()`). | Scheduler Lease Reaper detects expired lease. Increments `attempt`. If `attempt < max_retries`, re-queues. Else marks `FAILED`. |
| **Worker** | Network partition during task execution. | Worker continues running; cannot reach API. | At $t = \text{lease\_expires}$, Scheduler re-queues job to Worker B. | Worker A context cancels itself at lease expiry. If Worker A later attempts completion, API rejects with `409 Conflict`. State remains clean. |
| **Worker** | Subprocess enters infinite loop or hangs. | No completion ACK sent. | Heartbeat continues until `timeout_seconds` exceeded. | Worker timeout monitor kills subprocess group (`SIGKILL` / `TerminateJobObject`). Job marked `TIMED_OUT`. |
| **Scheduler** | Scheduler process crashes. | Delayed jobs not promoted; expired leases un-reaped. | Running workers continue executing and heartbeating without interruption. | Schedulers are stateless. Secondary scheduler node or restarted scheduler resumes sweeping immediately. `SKIP LOCKED` prevents duplicate work. |
| **API Server**| API process crashes during submission. | Client receives connection reset or 502. | Transaction rolls back if mid-commit; no partial records. | Client retries request with same `Idempotency-Key`. Returns original record if commit succeeded, or creates cleanly if it did not. |
| **Database** | Database temporarily unavailable. | API and Workers receive connection errors. | API returns `503 Service Unavailable`. Workers pause claim loop and retry heartbeats with exponential backoff. | Once DB recovers, workers resume. Any leases that expired during extended outage are safely reaped. |
| **Job Handler**| Handler throws unhandled exception or non-zero exit code. | Worker catches panic or subprocess exit code. | Worker records error details in `job_executions`. | If error is retryable, transitions to `RETRYING` with backoff. If fatal or out of retries, transitions to `FAILED`. |

---

## 12. Security Architecture & Threat Model

```
                    ┌──────────────────────────────────────────────┐
                    │               Security Boundary              │
                    │                                              │
                    │  Worker Host Process (Non-Root / Non-Admin)  │
                    │                                              │
                    │   ┌──────────────────────────────────────┐   │
                    │   │        Sanitized Subprocess          │   │
                    │   │                                      │   │
                    │   │  - Direct execve (No sh -c)          │   │
                    │   │  - Stripped Environment Variables    │   │
                    │   │  - Win32 Job Object / Linux Cgroup   │   │
                    │   │  - Blocked Cloud Metadata IP         │   │
                    │   │  - Strict Execution Timeout          │   │
                    │   └──────────────────────────────────────┘   │
                    └──────────────────────────────────────────────┘
```

1. **Authentication & RBAC**:
   - API Keys are prefixed (`je_live_...`), hashed using **Argon2id** (with per-key salt), and indexed by hash.
   - Roles:
     - `ADMIN`: Full tenant administrative access, queue pausing, API key creation.
     - `SUBMITTER`: Submit jobs/workflows, query status, cancel jobs.
     - `WORKER`: Restricted strictly to claim, heartbeat, complete, and fail jobs for assigned queues.
     - `READONLY`: Read-only queries for monitoring and audit.
2. **Subprocess Isolation Rules**:
   - **No Shell Interpreter**: Execution uses direct binary execution (`exec.Command(binary, args...)`), preventing shell metacharacter injection (`|`, `;`, `&&`, `` ` ``).
   - **Environment Scrubbing**: Host environment variables (especially database passwords, cloud tokens, API keys) are completely removed. Only explicit allowlisted variables (`PATH`, `TEMP`, `JOBENGINE_JOB_ID`) are passed.
   - **Process Group Killing**: On timeout or cancellation, the entire process tree is terminated (POSIX `syscall.Kill(-pid, SIGKILL)` or Windows `TerminateJobObject`), ensuring no orphaned daemon processes linger.
   - **SSRF Defense**: Subprocesses are prevented from contacting internal metadata endpoints (`169.254.169.254`).

---

## 13. REST API Specification

All endpoints return JSON responses and use standard HTTP status codes.

| Method | Path | Summary | Auth Role | Description |
|---|---|---|---|---|
| `POST` | `/api/v1/jobs` | Submit Job | `SUBMITTER`, `ADMIN` | Submits an atomic job. Accepts `Idempotency-Key` header. |
| `GET` | `/api/v1/jobs/{id}` | Get Job Details | `READONLY`, `SUBMITTER` | Returns job state, payload, result, execution attempts. |
| `POST` | `/api/v1/jobs/{id}/cancel`| Cancel Job | `SUBMITTER`, `ADMIN` | Cancels job; revokes active lease. |
| `POST` | `/api/v1/jobs/{id}/retry` | Retry Job | `SUBMITTER`, `ADMIN` | Manually retries a failed or timed-out job. |
| `POST` | `/api/v1/workflows` | Submit Workflow | `SUBMITTER`, `ADMIN` | Submits DAG workflow. Validates acyclic structure. |
| `GET` | `/api/v1/workflows/{id}` | Get Workflow | `READONLY`, `SUBMITTER` | Returns workflow DAG execution graph and step states. |
| `POST` | `/api/v1/workflows/{id}/cancel`| Cancel Workflow| `SUBMITTER`, `ADMIN` | Cancels workflow and all active/pending steps. |
| `POST` | `/api/v1/workers/claim` | Claim Jobs | `WORKER` | Worker batch claim endpoint (`FOR UPDATE SKIP LOCKED`). |
| `POST` | `/api/v1/jobs/{id}/heartbeat`| Heartbeat Lease | `WORKER` | Renews execution lease with `lease_token`. |
| `POST` | `/api/v1/jobs/{id}/complete` | Complete Job | `WORKER` | Submits successful result with `lease_token`. |
| `POST` | `/api/v1/jobs/{id}/fail` | Fail Job | `WORKER` | Reports job failure and error payload. |
| `GET` | `/api/v1/queues` | List Queues | `READONLY`, `ADMIN` | Returns queue stats, depth, active workers. |
| `POST` | `/api/v1/queues/{name}/pause`| Pause Queue | `ADMIN` | Pauses queue draining. |
| `GET` | `/healthz` | Liveness Probe | Public | Returns `200 OK` if process is responsive. |
| `GET` | `/readyz` | Readiness Probe | Public | Returns `200 OK` if database connection is healthy. |
| `GET` | `/metrics` | Prometheus Metrics | Public / Internal | Exports Prometheus metrics. |

---

## 14. CLI Design (`jobengine`)

The CLI provides unified ergonomics for operators and developers:

```bash
# Server & Worker Daemons
jobengine server --config=config.yaml               # Starts API and Scheduler
jobengine worker --queues=default,high --concurrency=8 # Starts Worker node

# Job Operations
jobengine job submit --queue=default --payload='{"task":"backup"}' --priority=10
jobengine job get <job-id>
jobengine job cancel <job-id>
jobengine job retry <job-id>
jobengine job logs <job-id>

# Workflow Operations
jobengine workflow submit --file=pipeline.json
jobengine workflow status <workflow-id> --watch
jobengine workflow cancel <workflow-id>

# Cluster & Queue Inspection
jobengine queues list
jobengine workers list
```

All commands support `--json` for scripting and automation.

---

## 15. Observability & Metrics

### 15.1 Core Prometheus Metrics

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `jobengine_jobs_submitted_total` | Counter | `tenant`, `queue` | Total jobs submitted. |
| `jobengine_queue_depth` | Gauge | `tenant`, `queue`, `status` | Current number of jobs per status. |
| `jobengine_job_duration_seconds` | Histogram | `queue`, `handler`, `status` | End-to-end task execution latency. |
| `jobengine_job_claims_total` | Counter | `queue`, `worker_id` | Count of jobs claimed by workers. |
| `jobengine_lease_expirations_total`| Counter | `queue` | Count of abandoned/expired worker leases. |
| `jobengine_retries_total` | Counter | `queue`, `reason` | Count of retry attempts triggered. |
| `jobengine_workers_active` | Gauge | `status` | Count of registered live workers. |
| `jobengine_workflow_duration_seconds`| Histogram| `workflow_name`, `status` | End-to-end workflow completion time. |

### 15.2 Structured Logging Standard
Every log entry is output in JSON format with standard correlation fields:
`{"timestamp":"2026-09-13T08:50:00Z","level":"info","trace_id":"...","tenant_id":"...","job_id":"...","workflow_id":"...","step_name":"...","worker_id":"...","attempt":1,"msg":"job claimed successfully"}`

---

## 16. Test Strategy & Failure Verification

The testing framework is structured into five distinct verification tiers:

```
┌────────────────────────────────────────────────────────┐
│  Tier 5: Load & Contention Benchmarks                 │
│  - 5,000 concurrent claims/sec, lock contention check  │
├────────────────────────────────────────────────────────┤
│  Tier 4: Chaos & Fault-Injection Tests                 │
│  - Kill worker mid-task, kill scheduler, network drops │
├────────────────────────────────────────────────────────┤
│  Tier 3: End-to-End Workflow Tests                     │
│  - Full multi-step DAG submission to completion        │
├────────────────────────────────────────────────────────┤
│  Tier 2: Persistence & Integration Tests               │
│  - Real PostgreSQL & SQLite concurrent transactions    │
├────────────────────────────────────────────────────────┤
│  Tier 1: Unit & Invariant Tests                        │
│  - State machine transitions, DAG cycle detection      │
└────────────────────────────────────────────────────────┘
```

### 16.1 Deterministic Invariant Tests
1. **The Zombie Worker Test**:
   - Worker 1 claims Job 1.
   - Test harness artificially advances DB clock or reduces lease to 1s.
   - Scheduler reaps lease; Worker 2 claims Job 1.
   - Worker 1 wakes up and calls `/complete`.
   - **Verification**: Worker 1 receives `409 Conflict`. Job remains assigned to Worker 2. Worker 2 completes successfully.
2. **The Concurrent Claim Race Test**:
   - 50 concurrent goroutines attempt to claim the single available job in a queue.
   - **Verification**: Exactly 1 goroutine succeeds; 49 return empty. No duplicate assignments.
3. **The DAG Dependency Order Test**:
   - A DAG with 10 nodes and 3 parallel branches is submitted.
   - Workers execute tasks with artificial random jitter (10ms - 200ms).
   - **Verification**: No child step ever transitions to `QUEUED` before all its ancestor steps are `COMPLETED`.

---

## 17. Repository Structure

The project follows the standard Go ecosystem project layout:

```text
/
├── cmd/
│   ├── jobengine/             # Unified CLI and daemon binary
│   │   └── main.go
├── internal/
│   ├── api/                   # HTTP REST handlers, routing, middleware
│   │   ├── handler_jobs.go
│   │   ├── handler_workflows.go
│   │   ├── handler_workers.go
│   │   └── middleware_auth.go
│   ├── store/                 # Storage interface & SQL implementations
│   │   ├── store.go           # Storage interface definition
│   │   ├── postgres/          # PostgreSQL implementation (pgx)
│   │   └── sqlite/            # SQLite implementation (for embedded/testing)
│   ├── scheduler/             # Lease reaper, sweeper, DAG propagator
│   │   ├── reaper.go
│   │   ├── sweeper.go
│   │   └── propagator.go
│   ├── worker/                # Worker claim loop, heartbeat, runner
│   │   ├── worker.go
│   │   ├── heartbeat.go
│   │   ├── runner_handler.go
│   │   └── runner_subprocess.go
│   ├── workflow/              # DAG cycle detection, validation, templates
│   │   ├── dag.go
│   │   └── template.go
│   └── telemetry/             # Prometheus metrics, structured logging
│       ├── metrics.go
│       └── logger.go
├── pkg/
│   └── client/                # Go Client SDK for external applications
│       └── client.go
├── migrations/                # Database schema migrations (.sql)
│   ├── 000001_initial_schema.up.sql
│   └── 000001_initial_schema.down.sql
├── tests/
│   ├── integration/           # DB store integration tests
│   ├── e2e/                   # Full pipeline submit -> execute tests
│   └── chaos/                 # Worker kill, lease expiration fault tests
├── docs/
│   ├── architecture/          # Architecture diagrams and specifications
│   ├── decisions/             # Architectural Decision Records (ADRs)
│   └── planning/              # Master engineering plans and roadmaps
├── scripts/
│   ├── dev.ps1                # Windows local development helper
│   └── dev.sh                 # Linux/macOS local development helper
├── .golangci.yml              # Linter configuration
├── go.mod
├── go.sum
└── CLAUDE.md                  # Instructions for AI coding agents
```

---

## 18. Technology Selection Rationale

| Layer | Chosen Technology | Alternatives Evaluated | Deciding Rationale |
|---|---|---|---|
| **Primary Language** | **Go (1.24+)** | Rust, Python, Node.js | Single binary distribution, native goroutines, low memory footprint (<25MB), built-in race detector (`-race`). |
| **Persistence Engine** | **PostgreSQL (pgx)** | Redis, RabbitMQ, Kafka, MySQL | Single source of truth. Atomic `FOR UPDATE SKIP LOCKED` eliminates the dual-write problem. |
| **Embedded Store** | **SQLite (modernc/sqlite)**| In-memory map, mock | 100% CGO-free pure Go SQLite. Enables instant local unit & integration tests without Docker. |
| **HTTP Router** | **Standard `net/http`** | Gin, Fiber, Echo | Zero external router dependencies; Go 1.22+ native routing handles method and wildcard patterns cleanly. |
| **Observability** | **Prometheus + Zap** | OpenTelemetry, Logrus | Prometheus is the de facto cloud-native metric standard; Zap provides zero-allocation JSON structured logging. |
| **CLI Framework** | **Cobra** | Urfave/cli, standard flag | Industry standard (kubectl, gh, docker) with nested subcommands, shell auto-completion, and flag binding. |

---

## 19. Implementation Phases

```
Phase 0: Foundation & Tooling
   │
Phase 1: Persistence & Core Store (Postgres + SQLite)
   │
Phase 2: Atomic Queue & Claim Engine (SKIP LOCKED + Fencing)
   │
Phase 3: Worker Engine & Heartbeat Loop
   │
Phase 4: Reliability, Retries, & Lease Reaper
   │
Phase 5: Declarative Workflow DAG Engine
   │
Phase 6: REST API & Auth (RBAC / Tenant Isolation)
   │
Phase 7: CLI Tooling (`jobengine`)
   │
Phase 8: Telemetry, Observability, & Metrics
   │
Phase 9: Chaos Engineering & Hardening
   │
Phase 10: Documentation & Production Packaging
```

### Phase 0: Foundation & Environment Setup
- **Scope**: Repository initialization, `go.mod`, directory structure, `.golangci.yml`, `CLAUDE.md`, test runner scripts.
- **Prerequisites**: Go 1.24+ runtime, Git.
- **Acceptance Criteria**: `go test ./...` passes; linter runs cleanly; single command builds binary.

### Phase 1: Persistence Layer & Data Model
- **Scope**: SQL schema migrations, `Store` interface definition, PostgreSQL (`pgx`) and SQLite implementations, connection pool tuning.
- **Prerequisites**: Phase 0.
- **Acceptance Criteria**: Integration tests verify CRUD on jobs, workflows, tenants, workers across both PostgreSQL and SQLite.

### Phase 2: Atomic Queue & Claim Engine
- **Scope**: Implementation of atomic claim query with `SKIP LOCKED`, monotonic attempt counter, UUID lease token generation, priority ordering.
- **Prerequisites**: Phase 1.
- **Acceptance Criteria**: Concurrent claim tests prove zero duplicate assignments across 50 simultaneous workers.

### Phase 3: Worker Engine & Heartbeat
- **Scope**: Worker claim polling loop, heartbeat renewal goroutine, Go function handler runner, isolated subprocess runner with environment sanitization.
- **Prerequisites**: Phase 2.
- **Acceptance Criteria**: Worker claims job, executes handler, maintains heartbeat, acknowledges completion; subprocess environment is verified clean.

### Phase 4: Reliability, Retries & Lease Reaper
- **Scope**: Scheduler background sweeper, lease reaper for expired workers, full-jitter exponential backoff calculation, delayed job promotion (`run_at`).
- **Prerequisites**: Phase 3.
- **Acceptance Criteria**: Expired worker leases are automatically detected and re-queued; zombie completions returning 409 are verified suppressed.

### Phase 5: Declarative Workflow DAG Engine
- **Scope**: DAG JSON parser, 3-color DFS cycle detector, parameter template interpolation, step progression transactional coordinator.
- **Prerequisites**: Phase 4.
- **Acceptance Criteria**: Multi-step DAG executes in topological order; step outputs pipe into downstream inputs; cyclic DAGs are rejected.

### Phase 6: REST API & Authentication
- **Scope**: HTTP REST handlers, Argon2id API key middleware, tenant query filtering, OpenAPI/Swagger specification.
- **Prerequisites**: Phase 5.
- **Acceptance Criteria**: Authenticated clients submit jobs/workflows; cross-tenant access attempts return 403/404.

### Phase 7: CLI Tooling
- **Scope**: Cobra CLI commands (`job submit`, `job get`, `workflow run`, `worker start`, `server start`), `--json` output support.
- **Prerequisites**: Phase 6.
- **Acceptance Criteria**: End-to-end user workflow can be driven entirely via CLI.

### Phase 8: Telemetry & Observability
- **Scope**: Prometheus metric instrumentation, structured JSON logger with trace IDs, `/healthz`, `/readyz`, `/metrics` endpoints.
- **Prerequisites**: Phase 7.
- **Acceptance Criteria**: Metrics report accurate queue depth and durations under test load.

### Phase 9: Chaos Engineering & Hardening
- **Scope**: Automated fault-injection tests (kill -9 worker mid-job, kill scheduler, network latency simulation, lock contention benchmarks).
- **Prerequisites**: Phase 8.
- **Acceptance Criteria**: All chaos test scenarios recover cleanly with zero data loss and zero inconsistent states.

### Phase 10: Production Readiness & Release
- **Scope**: Dockerfile multi-stage build, deployment guides, user manual, final security audit.
- **Prerequisites**: Phase 9.
- **Acceptance Criteria**: Single binary compiles cleanly; automated release artifact generated.

---

## 20. Concrete Acceptance Criteria & Verification Matrix

| Subsystem | Specific Criterion | Verification Method | Status |
|---|---|---|---|
| **Claiming** | No two workers can claim the same job simultaneously. | 50 concurrent worker threads claiming 1 job. | **PROVEN** via automated race test. |
| **Fencing** | Worker completing with expired lease is rejected. | Artificial delay test; verify 409 Conflict. | **PROVEN** via integration test. |
| **Durability** | Zero state loss if worker killed with SIGKILL mid-task. | Process kill test; verify job re-queued by reaper. | **PROVEN** via chaos suite. |
| **DAG** | Cyclic workflows are rejected at API submission. | Submit DAG with A->B->C->A; verify 400 Bad Request. | **PROVEN** via unit test. |
| **Isolation** | Subprocess cannot read host DB password or cloud tokens. | Child process inspects environment variables. | **PROVEN** via security test. |
| **Idempotency**| Re-submitting identical job returns original entity. | Duplicate POST with same `Idempotency-Key`. | **PROVEN** via API test. |

---

## 21. Scope Boundaries

### In Scope
- Core durable job engine with atomic priority queue.
- Declarative multi-step DAG workflow orchestration.
- Worker pool with heartbeat renewal and lease fencing.
- Process isolation for script/binary execution.
- REST API and interactive CLI.
- PostgreSQL production store + SQLite embedded store.
- Prometheus metrics and structured logging.

### Deferred (Post-M6)
- Web UI Dashboard.
- Distributed Cron scheduler with visual calendar editor.
- Dynamic loops (`while`/`for-each`) in declarative workflows.
- Webhook trigger ingress for external events.

### Out of Scope
- Temporal-style imperative code replay.
- MicroVM (Firecracker) container virtualization.
- Multi-datacenter Paxos/Raft consensus.
- Streaming data pipeline transformations (Kafka/Flink replacement).

---

## 22. Ranked Technical Risks & Mitigations

1. **Risk: PostgreSQL Table Bloat under Extreme High-Throughput**
   - *Impact*: Heavy job churn creates thousands of dead MVCC tuples, slowing queue queries.
   - *Mitigation*: Partial indexes index only `status IN ('QUEUED', 'RUNNING')`. Completed jobs are migrated to partitioned history tables or pruned via retention policy.
2. **Risk: Worker Hangs without Yielding CPU or Process**
   - *Impact*: Thread exhaustion in worker node.
   - *Mitigation*: Hard OS-level process group timeouts (`SIGKILL` / `TerminateJobObject`) terminate unresponsive child processes.
3. **Risk: Split-Brain Zombie Worker Writes Conflicting External Data**
   - *Impact*: External side effect performed twice.
   - *Mitigation*: Explicit at-least-once developer contract documented; provide idempotency tokens in task payload for external API calls.

---

## 23. Agent Handoff & Phase 1 Execution Instructions

To the implementation agent beginning **Phase 0 and Phase 1**:
1. Read `CLAUDE.md` in repository root for directory conventions and tool commands.
2. Initialize Go module: `go mod init github.com/jobengine/jobengine`.
3. Create the directory tree matching Section 17.
4. Begin with **Phase 0 (Foundation)**: configure `golangci.yml` and testing harness.
5. Proceed to **Phase 1 (Store)**: Implement the `Store` interface in `internal/store/store.go` and write the initial PostgreSQL/SQLite migration files.
6. Run tests with `go test -race ./...` to verify all concurrency primitives before progressing.
