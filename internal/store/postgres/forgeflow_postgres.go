package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
)

//go:embed schema.sql
var schemaDDL string

// PostgresStore implements store.Store backed by PostgreSQL and pgxpool.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// Open connects to the PostgreSQL database and applies the initial schema DDL if needed.
func Open(ctx context.Context, connString string) (*PostgresStore, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("invalid postgres connection string: %w", err)
	}

	// Sized for concurrency tests (e.g. 50 parallel workers)
	config.MaxConns = 60
	config.MinConns = 5
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	if _, err := pool.Exec(ctx, schemaDDL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to apply postgres schema ddl: %w", err)
	}

	return &PostgresStore{pool: pool}, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return true
	}
	return false
}

func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}

// -----------------------------------------------------------------------------
// Tenant Operations
// -----------------------------------------------------------------------------

func (s *PostgresStore) CreateTenant(ctx context.Context, tenant *domain.Tenant) error {
	if err := tenant.Validate(); err != nil {
		return err
	}
	now := time.Now().UTC()
	if tenant.CreatedAt.IsZero() {
		tenant.CreatedAt = now
	}

	query := `INSERT INTO tenants (id, name, status, created_at) VALUES ($1, $2, $3, $4)`
	_, err := s.pool.Exec(ctx, query, tenant.ID, tenant.Name, string(tenant.Status), tenant.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return store.ErrConflict
		}
		return fmt.Errorf("create tenant failed: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetTenant(ctx context.Context, id string) (*domain.Tenant, error) {
	if id == "" {
		return nil, store.ErrNotFound
	}

	query := `SELECT id, name, status, created_at FROM tenants WHERE id = $1`
	row := s.pool.QueryRow(ctx, query, id)

	var t domain.Tenant
	var statusStr string
	err := row.Scan(&t.ID, &t.Name, &statusStr, &t.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("get tenant failed: %w", err)
	}
	t.Status = domain.TenantStatus(statusStr)
	return &t, nil
}

// -----------------------------------------------------------------------------
// Queue Operations
// -----------------------------------------------------------------------------

func (s *PostgresStore) CreateQueue(ctx context.Context, queue *domain.Queue) error {
	if err := queue.Validate(); err != nil {
		return err
	}
	now := time.Now().UTC()
	if queue.CreatedAt.IsZero() {
		queue.CreatedAt = now
	}

	query := `INSERT INTO queues (tenant_id, name, is_paused, concurrency_limit, created_at) VALUES ($1, $2, $3, $4, $5)`
	_, err := s.pool.Exec(ctx, query, queue.TenantID, queue.Name, queue.IsPaused, queue.ConcurrencyLimit, queue.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return store.ErrConflict
		}
		return fmt.Errorf("create queue failed: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetQueue(ctx context.Context, tenantID, name string) (*domain.Queue, error) {
	if tenantID == "" || name == "" {
		return nil, store.ErrNotFound
	}

	query := `SELECT tenant_id, name, is_paused, concurrency_limit, created_at FROM queues WHERE tenant_id = $1 AND name = $2`
	row := s.pool.QueryRow(ctx, query, tenantID, name)

	var q domain.Queue
	err := row.Scan(&q.TenantID, &q.Name, &q.IsPaused, &q.ConcurrencyLimit, &q.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("get queue failed: %w", err)
	}
	return &q, nil
}

// -----------------------------------------------------------------------------
// Job Lifecycle & Claiming
// -----------------------------------------------------------------------------

func (s *PostgresStore) CreateJob(ctx context.Context, job *domain.Job) error {
	if err := job.ValidateCreation(); err != nil {
		return err
	}

	now := time.Now().UTC()
	job.CreatedAt = now
	job.UpdatedAt = now

	query := `
	INSERT INTO jobs (
		id, tenant_id, queue_name, workflow_id, workflow_step_id,
		status, priority, payload, attempt, fencing_generation,
		max_retries, retry_backoff_seconds, timeout_seconds, run_at,
		idempotency_key, created_at, updated_at
	) VALUES (
		$1, $2, $3, $4, $5,
		$6, $7, $8, $9, $10,
		$11, $12, $13, $14,
		$15, $16, $17
	)
	`
	_, err := s.pool.Exec(ctx, query,
		job.ID,
		job.TenantID,
		job.QueueName,
		job.WorkflowID,
		job.WorkflowStepID,
		string(job.Status),
		job.Priority,
		job.Payload,
		job.Attempt,
		job.FencingGeneration,
		job.MaxRetries,
		job.RetryBackoffSeconds,
		job.TimeoutSeconds,
		job.RunAt,
		job.IdempotencyKey,
		job.CreatedAt,
		job.UpdatedAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return store.ErrConflict
		}
		return fmt.Errorf("create job failed: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetJob(ctx context.Context, tenantID, id string) (*domain.Job, error) {
	if tenantID == "" || id == "" {
		return nil, store.ErrNotFound
	}

	query := `
	SELECT id, tenant_id, queue_name, workflow_id, workflow_step_id,
	       status, priority, payload, result, error_message,
	       attempt, fencing_generation, max_retries, retry_backoff_seconds,
	       timeout_seconds, run_at, lease_token, lease_expires_at, worker_id,
	       idempotency_key, created_at, updated_at, completed_at
	FROM jobs
	WHERE tenant_id = $1 AND id = $2
	`

	var j domain.Job
	var statusStr string
	err := s.pool.QueryRow(ctx, query, tenantID, id).Scan(
		&j.ID,
		&j.TenantID,
		&j.QueueName,
		&j.WorkflowID,
		&j.WorkflowStepID,
		&statusStr,
		&j.Priority,
		&j.Payload,
		&j.Result,
		&j.ErrorMessage,
		&j.Attempt,
		&j.FencingGeneration,
		&j.MaxRetries,
		&j.RetryBackoffSeconds,
		&j.TimeoutSeconds,
		&j.RunAt,
		&j.LeaseToken,
		&j.LeaseExpiresAt,
		&j.WorkerID,
		&j.IdempotencyKey,
		&j.CreatedAt,
		&j.UpdatedAt,
		&j.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("get job failed: %w", err)
	}
	j.Status = domain.JobStatus(statusStr)
	return &j, nil
}

// ClaimJobs atomically claims up to batchSize eligible jobs using PostgreSQL SKIP LOCKED.
func (s *PostgresStore) ClaimJobs(ctx context.Context, tenantID, workerID string, queues []string, batchSize int, leaseDuration time.Duration) ([]*domain.Job, error) {
	if tenantID == "" || workerID == "" || len(queues) == 0 || batchSize <= 0 || leaseDuration <= 0 {
		return nil, errors.New("invalid claim arguments")
	}

	now := time.Now().UTC()
	leaseExpiresAt := now.Add(leaseDuration)

	query := `
	WITH candidate AS (
		SELECT id
		FROM jobs
		WHERE tenant_id = $1
		  AND queue_name = ANY($2)
		  AND status = 'QUEUED'
		  AND run_at <= $3
		ORDER BY priority DESC, run_at ASC, id ASC
		FOR UPDATE SKIP LOCKED
		LIMIT $4
	)
	UPDATE jobs j
	SET status = 'RUNNING',
	    worker_id = $5,
	    lease_token = gen_random_uuid()::text,
	    lease_expires_at = $6,
	    attempt = attempt + 1,
	    fencing_generation = fencing_generation + 1,
	    updated_at = $7
	FROM candidate
	WHERE j.id = candidate.id
	RETURNING j.id, j.tenant_id, j.queue_name, j.workflow_id, j.workflow_step_id,
	          j.status, j.priority, j.payload, j.result, j.error_message,
	          j.attempt, j.fencing_generation, j.max_retries, j.retry_backoff_seconds,
	          j.timeout_seconds, j.run_at, j.lease_token, j.lease_expires_at, j.worker_id,
	          j.idempotency_key, j.created_at, j.updated_at, j.completed_at;
	`

	rows, err := s.pool.Query(ctx, query, tenantID, queues, now, batchSize, workerID, leaseExpiresAt, now)
	if err != nil {
		return nil, fmt.Errorf("claim jobs query failed: %w", err)
	}
	defer rows.Close()

	var claimed []*domain.Job
	for rows.Next() {
		var j domain.Job
		var statusStr string
		err := rows.Scan(
			&j.ID,
			&j.TenantID,
			&j.QueueName,
			&j.WorkflowID,
			&j.WorkflowStepID,
			&statusStr,
			&j.Priority,
			&j.Payload,
			&j.Result,
			&j.ErrorMessage,
			&j.Attempt,
			&j.FencingGeneration,
			&j.MaxRetries,
			&j.RetryBackoffSeconds,
			&j.TimeoutSeconds,
			&j.RunAt,
			&j.LeaseToken,
			&j.LeaseExpiresAt,
			&j.WorkerID,
			&j.IdempotencyKey,
			&j.CreatedAt,
			&j.UpdatedAt,
			&j.CompletedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan claimed job failed: %w", err)
		}
		j.Status = domain.JobStatus(statusStr)
		claimed = append(claimed, &j)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return claimed, nil
}

// -----------------------------------------------------------------------------
// Fencing-Protected Worker Mutations
// -----------------------------------------------------------------------------

func (s *PostgresStore) CompleteJob(ctx context.Context, tenantID, id string, fencingGen int64, leaseToken string, result []byte) error {
	if tenantID == "" || id == "" || leaseToken == "" {
		return store.ErrNotFound
	}

	now := time.Now().UTC()

	query := `
	UPDATE jobs
	SET status = 'COMPLETED',
	    result = $1,
	    completed_at = $2,
	    updated_at = $3
	WHERE tenant_id = $4
	  AND id = $5
	  AND fencing_generation = $6
	  AND lease_token = $7
	  AND status = 'RUNNING'
	`

	tag, err := s.pool.Exec(ctx, query, result, now, now, tenantID, id, fencingGen, leaseToken)
	if err != nil {
		return fmt.Errorf("complete job failed: %w", err)
	}

	if tag.RowsAffected() > 0 {
		return nil
	}

	return s.inspectJobFailureReason(ctx, tenantID, id, fencingGen, leaseToken)
}

func (s *PostgresStore) FailJob(ctx context.Context, tenantID, id string, fencingGen int64, leaseToken string, errMsg string, retryable bool, backoff time.Duration) error {
	if tenantID == "" || id == "" || leaseToken == "" {
		return store.ErrNotFound
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var currentStatus string
	var currentAttempt, maxRetries int
	var currentFencingGen int64
	var currentLeaseToken *string

	selQuery := `
	SELECT status, attempt, max_retries, fencing_generation, lease_token
	FROM jobs
	WHERE tenant_id = $1 AND id = $2
	`
	err = tx.QueryRow(ctx, selQuery, tenantID, id).Scan(&currentStatus, &currentAttempt, &maxRetries, &currentFencingGen, &currentLeaseToken)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrNotFound
		}
		return err
	}

	status := domain.JobStatus(currentStatus)
	if status.IsTerminal() {
		return store.ErrTerminalState
	}
	if status != domain.StatusRunning || currentFencingGen != fencingGen || currentLeaseToken == nil || *currentLeaseToken != leaseToken {
		return store.ErrLeaseLost
	}

	now := time.Now().UTC()

	if retryable && currentAttempt < maxRetries {
		nextRunAt := now.Add(backoff)
		updateQuery := `
		UPDATE jobs
		SET status = 'RETRYING',
		    run_at = $1,
		    lease_token = NULL,
		    error_message = $2,
		    updated_at = $3
		WHERE tenant_id = $4 AND id = $5 AND fencing_generation = $6 AND lease_token = $7 AND status = 'RUNNING'
		`
		_, err = tx.Exec(ctx, updateQuery, nextRunAt, errMsg, now, tenantID, id, fencingGen, leaseToken)
	} else {
		updateQuery := `
		UPDATE jobs
		SET status = 'FAILED',
		    completed_at = $1,
		    error_message = $2,
		    updated_at = $3
		WHERE tenant_id = $4 AND id = $5 AND fencing_generation = $6 AND lease_token = $7 AND status = 'RUNNING'
		`
		_, err = tx.Exec(ctx, updateQuery, now, errMsg, now, tenantID, id, fencingGen, leaseToken)
	}

	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) RenewLease(ctx context.Context, tenantID, id string, fencingGen int64, leaseToken string, duration time.Duration) error {
	if tenantID == "" || id == "" || leaseToken == "" {
		return store.ErrNotFound
	}

	now := time.Now().UTC()
	newExpiresAt := now.Add(duration)

	query := `
	UPDATE jobs
	SET lease_expires_at = $1,
	    updated_at = $2
	WHERE tenant_id = $3
	  AND id = $4
	  AND fencing_generation = $5
	  AND lease_token = $6
	  AND status = 'RUNNING'
	`

	tag, err := s.pool.Exec(ctx, query, newExpiresAt, now, tenantID, id, fencingGen, leaseToken)
	if err != nil {
		return fmt.Errorf("renew lease failed: %w", err)
	}

	if tag.RowsAffected() > 0 {
		return nil
	}

	return s.inspectJobFailureReason(ctx, tenantID, id, fencingGen, leaseToken)
}

func (s *PostgresStore) CancelJob(ctx context.Context, tenantID, id string) error {
	if tenantID == "" || id == "" {
		return store.ErrNotFound
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var currentStatus string
	err = tx.QueryRow(ctx, `SELECT status FROM jobs WHERE tenant_id = $1 AND id = $2`, tenantID, id).Scan(&currentStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrNotFound
		}
		return err
	}

	status := domain.JobStatus(currentStatus)
	if status.IsTerminal() {
		return store.ErrTerminalState
	}

	now := time.Now().UTC()
	query := `
	UPDATE jobs
	SET status = 'CANCELLED',
	    lease_token = NULL,
	    completed_at = $1,
	    updated_at = $2
	WHERE tenant_id = $3 AND id = $4
	`
	_, err = tx.Exec(ctx, query, now, now, tenantID, id)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) CreateExecution(ctx context.Context, exec *domain.JobExecution) error {
	if exec.JobID == "" || exec.ID == "" {
		return errors.New("execution id and job_id are required")
	}

	query := `
	INSERT INTO job_executions (
		id, job_id, attempt, fencing_generation, worker_id, status, error_message, started_at, finished_at, duration_ms
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	_, err := s.pool.Exec(ctx, query,
		exec.ID,
		exec.JobID,
		exec.Attempt,
		exec.FencingGeneration,
		exec.WorkerID,
		string(exec.Status),
		exec.ErrorMessage,
		exec.StartedAt,
		exec.FinishedAt,
		exec.DurationMs,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return store.ErrConflict
		}
		return fmt.Errorf("create execution failed: %w", err)
	}
	return nil
}

func (s *PostgresStore) inspectJobFailureReason(ctx context.Context, tenantID, id string, fencingGen int64, leaseToken string) error {
	var statusStr string
	var currentFencingGen int64
	var currentLeaseToken *string

	query := `SELECT status, fencing_generation, lease_token FROM jobs WHERE tenant_id = $1 AND id = $2`
	err := s.pool.QueryRow(ctx, query, tenantID, id).Scan(&statusStr, &currentFencingGen, &currentLeaseToken)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrNotFound
		}
		return err
	}

	status := domain.JobStatus(statusStr)
	if status.IsTerminal() {
		return store.ErrTerminalState
	}

	if status != domain.StatusRunning || currentFencingGen != fencingGen || currentLeaseToken == nil || *currentLeaseToken != leaseToken {
		return store.ErrLeaseLost
	}

	return store.ErrLeaseLost
}

// RequeueExpiredJob resets an expired or abandoned job back to QUEUED status.
func (s *PostgresStore) RequeueExpiredJob(ctx context.Context, tenantID, id string) error {
	query := `UPDATE jobs SET status = 'QUEUED', lease_token = NULL, worker_id = NULL, fencing_generation = fencing_generation + 1 WHERE tenant_id = $1 AND id = $2`
	_, err := s.pool.Exec(ctx, query, tenantID, id)
	return err
}

// ReapExpiredJobs scans for RUNNING jobs whose lease has expired and resets or times them out.
func (s *PostgresStore) ReapExpiredJobs(ctx context.Context, tenantID string, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 100
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var selQuery string
	var rows pgx.Rows
	if tenantID != "" {
		selQuery = `
		SELECT id, attempt, max_retries
		FROM jobs
		WHERE tenant_id = $1
		  AND status = 'RUNNING'
		  AND lease_expires_at IS NOT NULL
		  AND lease_expires_at < NOW()
		LIMIT $2
		FOR UPDATE SKIP LOCKED
		`
		rows, err = tx.Query(ctx, selQuery, tenantID, batchSize)
	} else {
		selQuery = `
		SELECT id, attempt, max_retries
		FROM jobs
		WHERE status = 'RUNNING'
		  AND lease_expires_at IS NOT NULL
		  AND lease_expires_at < NOW()
		LIMIT $1
		FOR UPDATE SKIP LOCKED
		`
		rows, err = tx.Query(ctx, selQuery, batchSize)
	}
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	type candidate struct {
		id         string
		attempt    int
		maxRetries int
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.attempt, &c.maxRetries); err != nil {
			return 0, err
		}
		candidates = append(candidates, c)
	}
	rows.Close()

	reaped := 0
	for _, c := range candidates {
		if c.attempt < c.maxRetries {
			upd := `
			UPDATE jobs
			SET status = 'QUEUED',
			    worker_id = NULL,
			    lease_token = NULL,
			    lease_expires_at = NULL,
			    fencing_generation = fencing_generation + 1,
			    updated_at = NOW()
			WHERE id = $1 AND status = 'RUNNING' AND lease_expires_at IS NOT NULL AND lease_expires_at <= NOW()
			`
			ct, err := tx.Exec(ctx, upd, c.id)
			if err != nil {
				return reaped, err
			}
			reaped += int(ct.RowsAffected())
		} else {
			upd := `
			UPDATE jobs
			SET status = 'TIMED_OUT',
			    error_message = 'lease expired and maximum retries exhausted',
			    worker_id = NULL,
			    lease_token = NULL,
			    completed_at = NOW(),
			    updated_at = NOW()
			WHERE id = $1 AND status = 'RUNNING' AND lease_expires_at IS NOT NULL AND lease_expires_at <= NOW()
			`
			ct, err := tx.Exec(ctx, upd, c.id)
			if err != nil {
				return reaped, err
			}
			reaped += int(ct.RowsAffected())
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return reaped, nil
}

// CreateWorkflow persists a new workflow along with its DAG step definitions atomically.
func (s *PostgresStore) CreateWorkflow(ctx context.Context, wf *domain.Workflow, steps []*domain.WorkflowStep) error {
	if err := wf.ValidateCreation(); err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	insertWF := `
	INSERT INTO workflows (id, tenant_id, name, status, idempotency_key, definition_json, context_data, error_message, created_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	_, err = tx.Exec(ctx, insertWF, wf.ID, wf.TenantID, wf.Name, string(wf.Status), wf.IdempotencyKey, wf.DefinitionJSON, wf.ContextData, wf.ErrorMessage, wf.CreatedAt)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "unique constraint") {
			return store.ErrConflict
		}
		return err
	}

	insertStep := `
	INSERT INTO workflow_steps (id, workflow_id, step_name, status, dependencies, handler, input_template, output_data, error_message, failure_policy, created_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`
	for _, step := range steps {
		stepCreated := step.CreatedAt
		if stepCreated.IsZero() {
			stepCreated = wf.CreatedAt
		}
		_, err = tx.Exec(ctx, insertStep, step.ID, wf.ID, step.StepName, string(step.Status), step.Dependencies, step.Handler, step.InputTemplate, step.OutputData, step.ErrorMessage, string(step.FailurePolicy), stepCreated)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// GetWorkflow fetches a workflow and all of its associated steps.
func (s *PostgresStore) GetWorkflow(ctx context.Context, tenantID, id string) (*domain.Workflow, []*domain.WorkflowStep, error) {
	var wf domain.Workflow
	var statusStr string

	query := `
	SELECT id, tenant_id, name, status, idempotency_key, definition_json, context_data, error_message, created_at, started_at, completed_at
	FROM workflows
	WHERE tenant_id = $1 AND id = $2
	`
	err := s.pool.QueryRow(ctx, query, tenantID, id).Scan(
		&wf.ID, &wf.TenantID, &wf.Name, &statusStr, &wf.IdempotencyKey,
		&wf.DefinitionJSON, &wf.ContextData, &wf.ErrorMessage, &wf.CreatedAt, &wf.StartedAt, &wf.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, store.ErrNotFound
		}
		return nil, nil, err
	}
	wf.Status = domain.WorkflowStatus(statusStr)

	stepQuery := `
	SELECT id, workflow_id, step_name, status, dependencies, handler, input_template, output_data, error_message, failure_policy, created_at, completed_at
	FROM workflow_steps
	WHERE workflow_id = $1
	ORDER BY created_at ASC
	`
	rows, err := s.pool.Query(ctx, stepQuery, id)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var steps []*domain.WorkflowStep
	for rows.Next() {
		var step domain.WorkflowStep
		var sStatusStr, sPolicyStr string

		err := rows.Scan(
			&step.ID, &step.WorkflowID, &step.StepName, &sStatusStr, &step.Dependencies,
			&step.Handler, &step.InputTemplate, &step.OutputData, &step.ErrorMessage, &sPolicyStr,
			&step.CreatedAt, &step.CompletedAt,
		)
		if err != nil {
			return nil, nil, err
		}
		step.Status = domain.JobStatus(sStatusStr)
		step.FailurePolicy = domain.FailurePolicy(sPolicyStr)
		steps = append(steps, &step)
	}

	return &wf, steps, nil
}

// UpdateWorkflowStatus modifies the status of an existing workflow.
func (s *PostgresStore) UpdateWorkflowStatus(ctx context.Context, tenantID, id string, status domain.WorkflowStatus, errMsg *string) error {
	var completedAt *time.Time
	if status.IsTerminal() {
		now := time.Now().UTC()
		completedAt = &now
	}

	query := `
	UPDATE workflows
	SET status = $1, error_message = $2, completed_at = COALESCE($3, completed_at)
	WHERE tenant_id = $4 AND id = $5 AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
	`
	ct, err := s.pool.Exec(ctx, query, string(status), errMsg, completedAt, tenantID, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		var currentStatus string
		err := s.pool.QueryRow(ctx, `SELECT status FROM workflows WHERE tenant_id = $1 AND id = $2`, tenantID, id).Scan(&currentStatus)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return store.ErrNotFound
			}
			return err
		}
		if domain.WorkflowStatus(currentStatus).IsTerminal() {
			return store.ErrTerminalState
		}
		return store.ErrNotFound
	}
	return nil
}

// UpdateWorkflowStep updates step execution status, output data, and error message.
func (s *PostgresStore) UpdateWorkflowStep(ctx context.Context, tenantID, stepID string, status domain.JobStatus, output []byte, errMsg *string) error {
	var completedAt *time.Time
	if status.IsTerminal() {
		now := time.Now().UTC()
		completedAt = &now
	}

	query := `
	UPDATE workflow_steps ws
	SET status = $1, output_data = $2, error_message = $3, completed_at = COALESCE($4, completed_at)
	FROM workflows w
	WHERE ws.workflow_id = w.id AND w.tenant_id = $5 AND ws.id = $6
	`
	ct, err := s.pool.Exec(ctx, query, string(status), output, errMsg, completedAt, tenantID, stepID)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

// GetExecutions retrieves all execution attempts for a given job.
func (s *PostgresStore) GetExecutions(ctx context.Context, tenantID, jobID string) ([]*domain.JobExecution, error) {
	query := `
	SELECT je.id, je.job_id, je.attempt, je.fencing_generation, je.worker_id, je.status, je.error_message, je.started_at, je.finished_at, je.duration_ms
	FROM job_executions je
	JOIN jobs j ON je.job_id = j.id
	WHERE j.tenant_id = $1 AND je.job_id = $2
	ORDER BY je.attempt ASC
	`
	rows, err := s.pool.Query(ctx, query, tenantID, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var execs []*domain.JobExecution
	for rows.Next() {
		var e domain.JobExecution
		var statusStr string

		err := rows.Scan(
			&e.ID, &e.JobID, &e.Attempt, &e.FencingGeneration, &e.WorkerID,
			&statusStr, &e.ErrorMessage, &e.StartedAt, &e.FinishedAt, &e.DurationMs,
		)
		if err != nil {
			return nil, err
		}
		e.Status = domain.JobStatus(statusStr)
		execs = append(execs, &e)
	}
	return execs, nil
}
