-- ForgeFlow Initial Schema Migration (PostgreSQL)

CREATE TABLE IF NOT EXISTS tenants (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS queues (
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name VARCHAR(64) NOT NULL,
    is_paused BOOLEAN NOT NULL DEFAULT FALSE,
    concurrency_limit INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS workflows (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    idempotency_key VARCHAR(128),
    definition_json JSONB NOT NULL,
    context_data JSONB NOT NULL DEFAULT '{}'::JSONB,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    CONSTRAINT uq_workflows_idempotency UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_workflows_tenant_status ON workflows(tenant_id, status);

CREATE TABLE IF NOT EXISTS workflow_steps (
    id VARCHAR(64) PRIMARY KEY,
    workflow_id VARCHAR(64) NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_name VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    dependencies JSONB NOT NULL DEFAULT '[]'::JSONB,
    handler VARCHAR(128) NOT NULL,
    input_template JSONB NOT NULL DEFAULT '{}'::JSONB,
    output_data JSONB,
    error_message TEXT,
    failure_policy VARCHAR(32) NOT NULL DEFAULT 'FAIL_WORKFLOW',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT uq_workflow_step UNIQUE (workflow_id, step_name)
);
CREATE INDEX IF NOT EXISTS idx_steps_workflow ON workflow_steps(workflow_id);

CREATE TABLE IF NOT EXISTS jobs (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    queue_name VARCHAR(64) NOT NULL,
    workflow_id VARCHAR(64) REFERENCES workflows(id) ON DELETE CASCADE,
    workflow_step_id VARCHAR(64) REFERENCES workflow_steps(id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL,
    priority INT NOT NULL DEFAULT 0,
    payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    result JSONB,
    error_message TEXT,
    attempt INT NOT NULL DEFAULT 0,
    fencing_generation BIGINT NOT NULL DEFAULT 0,
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
    FOREIGN KEY (tenant_id, queue_name) REFERENCES queues(tenant_id, name) ON DELETE RESTRICT,
    CONSTRAINT uq_jobs_idempotency UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_jobs_claimable ON jobs (queue_name, priority DESC, run_at ASC, id ASC)
WHERE status = 'QUEUED';

CREATE INDEX IF NOT EXISTS idx_jobs_running_lease ON jobs (lease_expires_at ASC)
WHERE status = 'RUNNING';

CREATE INDEX IF NOT EXISTS idx_jobs_delayed ON jobs (run_at ASC)
WHERE status IN ('SCHEDULED', 'RETRYING');

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_id ON jobs (tenant_id, id);

CREATE TABLE IF NOT EXISTS job_executions (
    id VARCHAR(64) PRIMARY KEY,
    job_id VARCHAR(64) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt INT NOT NULL,
    fencing_generation BIGINT NOT NULL,
    worker_id VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    duration_ms BIGINT
);
CREATE INDEX IF NOT EXISTS idx_executions_job ON job_executions(job_id, attempt);
