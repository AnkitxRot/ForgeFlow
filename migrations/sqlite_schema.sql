-- ForgeFlow Embedded Schema (SQLite)
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS queues (
    tenant_id TEXT NOT NULL,
    name TEXT NOT NULL,
    is_paused INTEGER NOT NULL DEFAULT 0,
    concurrency_limit INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, name),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS workflows (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    idempotency_key TEXT,
    definition_json TEXT NOT NULL,
    context_data TEXT NOT NULL DEFAULT '{}',
    error_message TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
    UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_workflows_tenant_status ON workflows(tenant_id, status);

CREATE TABLE IF NOT EXISTS workflow_steps (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    step_name TEXT NOT NULL,
    status TEXT NOT NULL,
    dependencies TEXT NOT NULL DEFAULT '[]',
    handler TEXT NOT NULL,
    input_template TEXT NOT NULL DEFAULT '{}',
    output_data TEXT,
    error_message TEXT,
    failure_policy TEXT NOT NULL DEFAULT 'FAIL_WORKFLOW',
    created_at TEXT NOT NULL,
    completed_at TEXT,
    FOREIGN KEY (workflow_id) REFERENCES workflows(id) ON DELETE CASCADE,
    UNIQUE (workflow_id, step_name)
);
CREATE INDEX IF NOT EXISTS idx_steps_workflow ON workflow_steps(workflow_id);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    workflow_id TEXT,
    workflow_step_id TEXT,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    payload TEXT NOT NULL DEFAULT '{}',
    result TEXT,
    error_message TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    fencing_generation INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    retry_backoff_seconds INTEGER NOT NULL DEFAULT 5,
    timeout_seconds INTEGER NOT NULL DEFAULT 300,
    run_at TEXT NOT NULL,
    lease_token TEXT,
    lease_expires_at TEXT,
    worker_id TEXT,
    idempotency_key TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    FOREIGN KEY (tenant_id, queue_name) REFERENCES queues(tenant_id, name) ON DELETE RESTRICT,
    FOREIGN KEY (workflow_id) REFERENCES workflows(id) ON DELETE CASCADE,
    FOREIGN KEY (workflow_step_id) REFERENCES workflow_steps(id) ON DELETE CASCADE,
    UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_jobs_claimable ON jobs (tenant_id, queue_name, priority DESC, run_at ASC, id ASC)
WHERE status = 'QUEUED';

CREATE INDEX IF NOT EXISTS idx_jobs_running_lease ON jobs (lease_expires_at ASC)
WHERE status = 'RUNNING';

CREATE INDEX IF NOT EXISTS idx_jobs_delayed ON jobs (run_at ASC)
WHERE status IN ('SCHEDULED', 'RETRYING');

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_id ON jobs (tenant_id, id);

CREATE TABLE IF NOT EXISTS job_executions (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    fencing_generation INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    duration_ms INTEGER,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_executions_job ON job_executions(job_id, attempt);
