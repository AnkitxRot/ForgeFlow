package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/google/uuid"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
)

//go:embed schema.sql
var schemaDDL string

// SQLiteStore implements store.Store backed by pure-Go modernc.org/sqlite.
type SQLiteStore struct {
	db *sql.DB
}

// Open initializes a SQLiteStore with standard WAL mode and foreign keys enabled.
func Open(dsn string) (*SQLiteStore, error) {
	// Ensure recommended WAL and busy timeout pragmas if not already configured in DSN
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	pragmas := "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_pragma=synchronous(NORMAL)"
	finalDSN := fmt.Sprintf("%s%s%s", dsn, separator, pragmas)

	db, err := sql.Open("sqlite", finalDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// SQLite in WAL mode requires single-writer serialization to prevent SQLITE_BUSY deadlocks.
	// Setting MaxOpenConns(1) queues concurrent operations safely in the Go driver.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping sqlite database: %w", err)
	}

	if _, err := db.Exec(schemaDDL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to execute sqlite schema ddl: %w", err)
	}

	return &SQLiteStore{db: db}, nil
}

func isUniqueConstraintErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") || strings.Contains(msg, "constraint failed")
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

// -----------------------------------------------------------------------------
// Tenant Operations
// -----------------------------------------------------------------------------

func (s *SQLiteStore) CreateTenant(ctx context.Context, tenant *domain.Tenant) error {
	if err := tenant.Validate(); err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if tenant.CreatedAt.IsZero() {
		tenant.CreatedAt = time.Now().UTC()
	}

	query := `INSERT INTO tenants (id, name, status, created_at) VALUES (?, ?, ?, ?)`
	_, err := s.db.ExecContext(ctx, query, tenant.ID, tenant.Name, string(tenant.Status), now)
	if err != nil {
		if isUniqueConstraintErr(err) {
			return store.ErrConflict
		}
		return fmt.Errorf("create tenant failed: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetTenant(ctx context.Context, id string) (*domain.Tenant, error) {
	if id == "" {
		return nil, store.ErrNotFound
	}

	query := `SELECT id, name, status, created_at FROM tenants WHERE id = ?`
	row := s.db.QueryRowContext(ctx, query, id)

	var t domain.Tenant
	var statusStr, createdAtStr string
	err := row.Scan(&t.ID, &t.Name, &statusStr, &createdAtStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("get tenant failed: %w", err)
	}
	t.Status = domain.TenantStatus(statusStr)
	t.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAtStr)
	return &t, nil
}

// -----------------------------------------------------------------------------
// Queue Operations
// -----------------------------------------------------------------------------

func (s *SQLiteStore) CreateQueue(ctx context.Context, queue *domain.Queue) error {
	if err := queue.Validate(); err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if queue.CreatedAt.IsZero() {
		queue.CreatedAt = time.Now().UTC()
	}

	isPausedInt := 0
	if queue.IsPaused {
		isPausedInt = 1
	}

	query := `INSERT INTO queues (tenant_id, name, is_paused, concurrency_limit, created_at) VALUES (?, ?, ?, ?, ?)`
	_, err := s.db.ExecContext(ctx, query, queue.TenantID, queue.Name, isPausedInt, queue.ConcurrencyLimit, now)
	if err != nil {
		if isUniqueConstraintErr(err) {
			return store.ErrConflict
		}
		return fmt.Errorf("create queue failed: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetQueue(ctx context.Context, tenantID, name string) (*domain.Queue, error) {
	if tenantID == "" || name == "" {
		return nil, store.ErrNotFound
	}

	query := `SELECT tenant_id, name, is_paused, concurrency_limit, created_at FROM queues WHERE tenant_id = ? AND name = ?`
	row := s.db.QueryRowContext(ctx, query, tenantID, name)

	var q domain.Queue
	var isPausedInt int
	var createdAtStr string
	err := row.Scan(&q.TenantID, &q.Name, &isPausedInt, &q.ConcurrencyLimit, &createdAtStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("get queue failed: %w", err)
	}
	q.IsPaused = isPausedInt == 1
	q.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAtStr)
	return &q, nil
}

// -----------------------------------------------------------------------------
// Job Operations
// -----------------------------------------------------------------------------

func (s *SQLiteStore) CreateJob(ctx context.Context, job *domain.Job) error {
	if err := job.ValidateCreation(); err != nil {
		return err
	}

	now := time.Now().UTC()
	job.CreatedAt = now
	job.UpdatedAt = now

	nowStr := now.Format(time.RFC3339Nano)
	runAtStr := job.RunAt.Format(time.RFC3339Nano)

	query := `
	INSERT INTO jobs (
		id, tenant_id, queue_name, workflow_id, workflow_step_id,
		status, priority, payload, attempt, fencing_generation,
		max_retries, retry_backoff_seconds, timeout_seconds, run_at,
		idempotency_key, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := s.db.ExecContext(ctx, query,
		job.ID,
		job.TenantID,
		job.QueueName,
		job.WorkflowID,
		job.WorkflowStepID,
		string(job.Status),
		job.Priority,
		string(job.Payload),
		job.Attempt,
		job.FencingGeneration,
		job.MaxRetries,
		job.RetryBackoffSeconds,
		job.TimeoutSeconds,
		runAtStr,
		job.IdempotencyKey,
		nowStr,
		nowStr,
	)
	if err != nil {
		if isUniqueConstraintErr(err) {
			return store.ErrConflict
		}
		return fmt.Errorf("create job failed: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetJob(ctx context.Context, tenantID, id string) (*domain.Job, error) {
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
	WHERE tenant_id = ? AND id = ?
	`

	row := s.db.QueryRowContext(ctx, query, tenantID, id)

	var j domain.Job
	var statusStr, payloadStr string
	var resultSql, errorMsgSql, leaseTokenSql, workerIdSql, idempotencyKeySql sql.NullString
	var workflowIdSql, workflowStepIdSql sql.NullString
	var runAtStr, createdAtStr, updatedAtStr string
	var leaseExpiresAtSql, completedAtSql sql.NullString

	err := row.Scan(
		&j.ID,
		&j.TenantID,
		&j.QueueName,
		&workflowIdSql,
		&workflowStepIdSql,
		&statusStr,
		&j.Priority,
		&payloadStr,
		&resultSql,
		&errorMsgSql,
		&j.Attempt,
		&j.FencingGeneration,
		&j.MaxRetries,
		&j.RetryBackoffSeconds,
		&j.TimeoutSeconds,
		&runAtStr,
		&leaseTokenSql,
		&leaseExpiresAtSql,
		&workerIdSql,
		&idempotencyKeySql,
		&createdAtStr,
		&updatedAtStr,
		&completedAtSql,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("get job failed: %w", err)
	}

	j.Status = domain.JobStatus(statusStr)
	j.Payload = []byte(payloadStr)
	if resultSql.Valid {
		j.Result = []byte(resultSql.String)
	}
	if errorMsgSql.Valid {
		j.ErrorMessage = &errorMsgSql.String
	}
	if workflowIdSql.Valid {
		j.WorkflowID = &workflowIdSql.String
	}
	if workflowStepIdSql.Valid {
		j.WorkflowStepID = &workflowStepIdSql.String
	}
	if leaseTokenSql.Valid {
		j.LeaseToken = &leaseTokenSql.String
	}
	if workerIdSql.Valid {
		j.WorkerID = &workerIdSql.String
	}
	if idempotencyKeySql.Valid {
		j.IdempotencyKey = &idempotencyKeySql.String
	}

	j.RunAt, _ = time.Parse(time.RFC3339Nano, runAtStr)
	j.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAtStr)
	j.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedAtStr)

	if leaseExpiresAtSql.Valid {
		t, _ := time.Parse(time.RFC3339Nano, leaseExpiresAtSql.String)
		j.LeaseExpiresAt = &t
	}
	if completedAtSql.Valid {
		t, _ := time.Parse(time.RFC3339Nano, completedAtSql.String)
		j.CompletedAt = &t
	}

	return &j, nil
}

// ClaimJobs atomically claims up to batchSize eligible jobs matching the specified queues in SQLite.
// Note: SQLite uses serialized transactions. Real high-concurrency non-blocking queue claims
// are tested exclusively against PostgreSQL using FOR UPDATE SKIP LOCKED.
func (s *SQLiteStore) ClaimJobs(ctx context.Context, tenantID, workerID string, queues []string, batchSize int, leaseDuration time.Duration) ([]*domain.Job, error) {
	if tenantID == "" || workerID == "" || len(queues) == 0 || batchSize <= 0 || leaseDuration <= 0 {
		return nil, errors.New("invalid claim arguments")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)
	leaseExpiresStr := now.Add(leaseDuration).Format(time.RFC3339Nano)

	placeholders := make([]string, len(queues))
	args := []any{tenantID}
	for i, q := range queues {
		placeholders[i] = "?"
		args = append(args, q)
	}
	args = append(args, nowStr, batchSize)

	selectQuery := fmt.Sprintf(`
	SELECT id
	FROM jobs
	WHERE tenant_id = ?
	  AND queue_name IN (%s)
	  AND status = 'QUEUED'
	  AND run_at <= ?
	ORDER BY priority DESC, run_at ASC, id ASC
	LIMIT ?
	`, strings.Join(placeholders, ","))

	rows, err := tx.QueryContext(ctx, selectQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candidateIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		candidateIDs = append(candidateIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(candidateIDs) == 0 {
		return []*domain.Job{}, nil
	}

	var claimedJobs []*domain.Job
	for _, id := range candidateIDs {
		leaseToken := uuid.NewString()
		updateQuery := `
		UPDATE jobs
		SET status = 'RUNNING',
		    worker_id = ?,
		    lease_token = ?,
		    lease_expires_at = ?,
		    attempt = attempt + 1,
		    fencing_generation = fencing_generation + 1,
		    updated_at = ?
		WHERE tenant_id = ? AND id = ? AND status = 'QUEUED'
		`
		res, err := tx.ExecContext(ctx, updateQuery, workerID, leaseToken, leaseExpiresStr, nowStr, tenantID, id)
		if err != nil {
			return nil, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected == 1 {
			var j domain.Job
			var statusStr, payloadStr string
			var resultSql, errorMsgSql, leaseTokenSql, workerIdSql, idempotencyKeySql sql.NullString
			var workflowIdSql, workflowStepIdSql sql.NullString
			var runAtStr, createdAtStr, updatedAtStr string
			var leaseExpiresAtSql, completedAtSql sql.NullString

			fetchQuery := `
			SELECT id, tenant_id, queue_name, workflow_id, workflow_step_id,
			       status, priority, payload, result, error_message,
			       attempt, fencing_generation, max_retries, retry_backoff_seconds,
			       timeout_seconds, run_at, lease_token, lease_expires_at, worker_id,
			       idempotency_key, created_at, updated_at, completed_at
			FROM jobs
			WHERE tenant_id = ? AND id = ?
			`
			err := tx.QueryRowContext(ctx, fetchQuery, tenantID, id).Scan(
				&j.ID, &j.TenantID, &j.QueueName, &workflowIdSql, &workflowStepIdSql,
				&statusStr, &j.Priority, &payloadStr, &resultSql, &errorMsgSql,
				&j.Attempt, &j.FencingGeneration, &j.MaxRetries, &j.RetryBackoffSeconds,
				&j.TimeoutSeconds, &runAtStr, &leaseTokenSql, &leaseExpiresAtSql, &workerIdSql,
				&idempotencyKeySql, &createdAtStr, &updatedAtStr, &completedAtSql,
			)
			if err != nil {
				return nil, err
			}
			j.Status = domain.JobStatus(statusStr)
			j.Payload = []byte(payloadStr)
			if resultSql.Valid {
				j.Result = []byte(resultSql.String)
			}
			if leaseTokenSql.Valid {
				j.LeaseToken = &leaseTokenSql.String
			}
			if workerIdSql.Valid {
				j.WorkerID = &workerIdSql.String
			}
			j.RunAt, _ = time.Parse(time.RFC3339Nano, runAtStr)
			j.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAtStr)
			j.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedAtStr)
			if leaseExpiresAtSql.Valid {
				t, _ := time.Parse(time.RFC3339Nano, leaseExpiresAtSql.String)
				j.LeaseExpiresAt = &t
			}
			claimedJobs = append(claimedJobs, &j)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimedJobs, nil
}

// -----------------------------------------------------------------------------
// Fencing-Protected Worker Mutations
// -----------------------------------------------------------------------------

func (s *SQLiteStore) CompleteJob(ctx context.Context, tenantID, id string, fencingGen int64, leaseToken string, result []byte) error {
	if tenantID == "" || id == "" || leaseToken == "" {
		return store.ErrNotFound
	}

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)
	resultStr := string(result)

	query := `
	UPDATE jobs
	SET status = 'COMPLETED',
	    result = ?,
	    completed_at = ?,
	    updated_at = ?
	WHERE tenant_id = ?
	  AND id = ?
	  AND fencing_generation = ?
	  AND lease_token = ?
	  AND status = 'RUNNING'
	`

	res, err := s.db.ExecContext(ctx, query, resultStr, nowStr, nowStr, tenantID, id, fencingGen, leaseToken)
	if err != nil {
		return fmt.Errorf("complete job update failed: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows > 0 {
		return nil
	}

	// Diagnostic query to return the precise domain error
	return s.inspectJobFailureReason(ctx, tenantID, id, fencingGen, leaseToken)
}

func (s *SQLiteStore) FailJob(ctx context.Context, tenantID, id string, fencingGen int64, leaseToken string, errMsg string, retryable bool, backoff time.Duration) error {
	if tenantID == "" || id == "" || leaseToken == "" {
		return store.ErrNotFound
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// First verify ownership and retrieve attempt/max_retries
	var currentStatus string
	var currentAttempt, maxRetries int
	var currentFencingGen int64
	var currentLeaseToken sql.NullString

	selQuery := `
	SELECT status, attempt, max_retries, fencing_generation, lease_token
	FROM jobs
	WHERE tenant_id = ? AND id = ?
	`
	err = tx.QueryRowContext(ctx, selQuery, tenantID, id).Scan(&currentStatus, &currentAttempt, &maxRetries, &currentFencingGen, &currentLeaseToken)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return store.ErrNotFound
		}
		return err
	}

	status := domain.JobStatus(currentStatus)
	if status.IsTerminal() {
		return store.ErrTerminalState
	}
	if status != domain.StatusRunning || currentFencingGen != fencingGen || !currentLeaseToken.Valid || currentLeaseToken.String != leaseToken {
		return store.ErrLeaseLost
	}

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	if retryable && currentAttempt < maxRetries {
		nextRunAt := now.Add(backoff).Format(time.RFC3339Nano)
		updateQuery := `
		UPDATE jobs
		SET status = 'RETRYING',
		    run_at = ?,
		    lease_token = NULL,
		    error_message = ?,
		    updated_at = ?
		WHERE tenant_id = ? AND id = ? AND fencing_generation = ? AND lease_token = ? AND status = 'RUNNING'
		`
		_, err = tx.ExecContext(ctx, updateQuery, nextRunAt, errMsg, nowStr, tenantID, id, fencingGen, leaseToken)
	} else {
		updateQuery := `
		UPDATE jobs
		SET status = 'FAILED',
		    completed_at = ?,
		    error_message = ?,
		    updated_at = ?
		WHERE tenant_id = ? AND id = ? AND fencing_generation = ? AND lease_token = ? AND status = 'RUNNING'
		`
		_, err = tx.ExecContext(ctx, updateQuery, nowStr, errMsg, nowStr, tenantID, id, fencingGen, leaseToken)
	}

	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) RenewLease(ctx context.Context, tenantID, id string, fencingGen int64, leaseToken string, duration time.Duration) error {
	if tenantID == "" || id == "" || leaseToken == "" {
		return store.ErrNotFound
	}

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)
	newExpiryStr := now.Add(duration).Format(time.RFC3339Nano)

	query := `
	UPDATE jobs
	SET lease_expires_at = ?,
	    updated_at = ?
	WHERE tenant_id = ?
	  AND id = ?
	  AND fencing_generation = ?
	  AND lease_token = ?
	  AND status = 'RUNNING'
	`

	res, err := s.db.ExecContext(ctx, query, newExpiryStr, nowStr, tenantID, id, fencingGen, leaseToken)
	if err != nil {
		return fmt.Errorf("renew lease update failed: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows > 0 {
		return nil
	}

	return s.inspectJobFailureReason(ctx, tenantID, id, fencingGen, leaseToken)
}

func (s *SQLiteStore) CancelJob(ctx context.Context, tenantID, id string) error {
	if tenantID == "" || id == "" {
		return store.ErrNotFound
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var currentStatus string
	err = tx.QueryRowContext(ctx, `SELECT status FROM jobs WHERE tenant_id = ? AND id = ?`, tenantID, id).Scan(&currentStatus)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return store.ErrNotFound
		}
		return err
	}

	status := domain.JobStatus(currentStatus)
	if status.IsTerminal() {
		return store.ErrTerminalState
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	query := `
	UPDATE jobs
	SET status = 'CANCELLED',
	    lease_token = NULL,
	    completed_at = ?,
	    updated_at = ?
	WHERE tenant_id = ? AND id = ?
	`
	_, err = tx.ExecContext(ctx, query, now, now, tenantID, id)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) CreateExecution(ctx context.Context, exec *domain.JobExecution) error {
	if exec.JobID == "" || exec.ID == "" {
		return errors.New("execution id and job_id are required")
	}

	startedAtStr := exec.StartedAt.Format(time.RFC3339Nano)
	var finishedAtSql sql.NullString
	if exec.FinishedAt != nil {
		finishedAtSql = sql.NullString{String: exec.FinishedAt.Format(time.RFC3339Nano), Valid: true}
	}

	query := `
	INSERT INTO job_executions (
		id, job_id, attempt, fencing_generation, worker_id, status, error_message, started_at, finished_at, duration_ms
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	_, err := s.db.ExecContext(ctx, query,
		exec.ID,
		exec.JobID,
		exec.Attempt,
		exec.FencingGeneration,
		exec.WorkerID,
		string(exec.Status),
		exec.ErrorMessage,
		startedAtStr,
		finishedAtSql,
		exec.DurationMs,
	)
	if err != nil {
		if isUniqueConstraintErr(err) {
			return store.ErrConflict
		}
		return fmt.Errorf("create execution failed: %w", err)
	}
	return nil
}

// inspectJobFailureReason runs a diagnostic query when a conditional update affected 0 rows,
// determining whether the root cause was ErrNotFound, ErrTerminalState, or ErrLeaseLost.
func (s *SQLiteStore) inspectJobFailureReason(ctx context.Context, tenantID, id string, fencingGen int64, leaseToken string) error {
	var statusStr string
	var currentFencingGen int64
	var currentLeaseToken sql.NullString

	query := `SELECT status, fencing_generation, lease_token FROM jobs WHERE tenant_id = ? AND id = ?`
	err := s.db.QueryRowContext(ctx, query, tenantID, id).Scan(&statusStr, &currentFencingGen, &currentLeaseToken)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return store.ErrNotFound
		}
		return err
	}

	status := domain.JobStatus(statusStr)
	if status.IsTerminal() {
		return store.ErrTerminalState
	}

	if status != domain.StatusRunning || currentFencingGen != fencingGen || !currentLeaseToken.Valid || currentLeaseToken.String != leaseToken {
		return store.ErrLeaseLost
	}

	return store.ErrLeaseLost
}

// ReapExpiredJobs scans for RUNNING jobs whose lease has expired and resets or times them out.
func (s *SQLiteStore) ReapExpiredJobs(ctx context.Context, tenantID string, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var query string
	var rows *sql.Rows
	if tenantID != "" {
		query = `
		SELECT id, attempt, max_retries
		FROM jobs
		WHERE tenant_id = ?
		  AND status = 'RUNNING'
		  AND lease_expires_at IS NOT NULL
		  AND lease_expires_at < ?
		LIMIT ?
		`
		rows, err = tx.QueryContext(ctx, query, tenantID, nowStr, batchSize)
	} else {
		query = `
		SELECT id, attempt, max_retries
		FROM jobs
		WHERE status = 'RUNNING'
		  AND lease_expires_at IS NOT NULL
		  AND lease_expires_at < ?
		LIMIT ?
		`
		rows, err = tx.QueryContext(ctx, query, nowStr, batchSize)
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
	_ = rows.Close()

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
			    updated_at = ?
			WHERE id = ? AND status = 'RUNNING' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?
			`
			res, err := tx.ExecContext(ctx, upd, nowStr, c.id, nowStr)
			if err != nil {
				return reaped, err
			}
			aff, _ := res.RowsAffected()
			reaped += int(aff)
		} else {
			errMsg := "lease expired and maximum retries exhausted"
			upd := `
			UPDATE jobs
			SET status = 'TIMED_OUT',
			    error_message = ?,
			    worker_id = NULL,
			    lease_token = NULL,
			    completed_at = ?,
			    updated_at = ?
			WHERE id = ? AND status = 'RUNNING' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?
			`
			res, err := tx.ExecContext(ctx, upd, errMsg, nowStr, nowStr, c.id, nowStr)
			if err != nil {
				return reaped, err
			}
			aff, _ := res.RowsAffected()
			reaped += int(aff)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return reaped, nil
}

// CreateWorkflow persists a new workflow along with all its step definitions atomically.
func (s *SQLiteStore) CreateWorkflow(ctx context.Context, wf *domain.Workflow, steps []*domain.WorkflowStep) error {
	if err := wf.ValidateCreation(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	nowStr := wf.CreatedAt.Format(time.RFC3339Nano)
	insertWF := `
	INSERT INTO workflows (id, tenant_id, name, status, idempotency_key, definition_json, context_data, error_message, created_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	_, err = tx.ExecContext(ctx, insertWF, wf.ID, wf.TenantID, wf.Name, string(wf.Status), wf.IdempotencyKey, string(wf.DefinitionJSON), string(wf.ContextData), wf.ErrorMessage, nowStr)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return store.ErrConflict
		}
		return err
	}

	insertStep := `
	INSERT INTO workflow_steps (id, workflow_id, step_name, status, dependencies, handler, input_template, output_data, error_message, failure_policy, created_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	for _, step := range steps {
		depsJSON, err := json.Marshal(step.Dependencies)
		if err != nil {
			return err
		}
		stepCreated := step.CreatedAt
		if stepCreated.IsZero() {
			stepCreated = wf.CreatedAt
		}
		_, err = tx.ExecContext(ctx, insertStep, step.ID, wf.ID, step.StepName, string(step.Status), string(depsJSON), step.Handler, string(step.InputTemplate), string(step.OutputData), step.ErrorMessage, string(step.FailurePolicy), stepCreated.Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetWorkflow fetches a workflow and all of its associated steps.
func (s *SQLiteStore) GetWorkflow(ctx context.Context, tenantID, id string) (*domain.Workflow, []*domain.WorkflowStep, error) {
	var wf domain.Workflow
	var statusStr, defStr, ctxStr, createdAtStr string
	var idempSql, errMsgSql, startedAtSql, completedAtSql sql.NullString

	query := `
	SELECT id, tenant_id, name, status, idempotency_key, definition_json, context_data, error_message, created_at, started_at, completed_at
	FROM workflows
	WHERE tenant_id = ? AND id = ?
	`
	err := s.db.QueryRowContext(ctx, query, tenantID, id).Scan(
		&wf.ID, &wf.TenantID, &wf.Name, &statusStr, &idempSql,
		&defStr, &ctxStr, &errMsgSql, &createdAtStr, &startedAtSql, &completedAtSql,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, store.ErrNotFound
		}
		return nil, nil, err
	}

	wf.Status = domain.WorkflowStatus(statusStr)
	wf.DefinitionJSON = []byte(defStr)
	wf.ContextData = []byte(ctxStr)
	if idempSql.Valid {
		wf.IdempotencyKey = &idempSql.String
	}
	if errMsgSql.Valid {
		wf.ErrorMessage = &errMsgSql.String
	}
	wf.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAtStr)
	if startedAtSql.Valid {
		t, _ := time.Parse(time.RFC3339Nano, startedAtSql.String)
		wf.StartedAt = &t
	}
	if completedAtSql.Valid {
		t, _ := time.Parse(time.RFC3339Nano, completedAtSql.String)
		wf.CompletedAt = &t
	}

	// Fetch steps
	stepQuery := `
	SELECT id, workflow_id, step_name, status, dependencies, handler, input_template, output_data, error_message, failure_policy, created_at, completed_at
	FROM workflow_steps
	WHERE workflow_id = ?
	ORDER BY created_at ASC
	`
	rows, err := s.db.QueryContext(ctx, stepQuery, id)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var steps []*domain.WorkflowStep
	for rows.Next() {
		var step domain.WorkflowStep
		var sStatusStr, sDepsStr, sInputStr, sPolicyStr, sCreatedStr string
		var sOutputSql, sErrSql, sCompletedSql sql.NullString

		err := rows.Scan(
			&step.ID, &step.WorkflowID, &step.StepName, &sStatusStr, &sDepsStr,
			&step.Handler, &sInputStr, &sOutputSql, &sErrSql, &sPolicyStr,
			&sCreatedStr, &sCompletedSql,
		)
		if err != nil {
			return nil, nil, err
		}
		step.Status = domain.JobStatus(sStatusStr)
		_ = json.Unmarshal([]byte(sDepsStr), &step.Dependencies)
		step.InputTemplate = []byte(sInputStr)
		if sOutputSql.Valid {
			step.OutputData = []byte(sOutputSql.String)
		}
		if sErrSql.Valid {
			step.ErrorMessage = &sErrSql.String
		}
		step.FailurePolicy = domain.FailurePolicy(sPolicyStr)
		step.CreatedAt, _ = time.Parse(time.RFC3339Nano, sCreatedStr)
		if sCompletedSql.Valid {
			t, _ := time.Parse(time.RFC3339Nano, sCompletedSql.String)
			step.CompletedAt = &t
		}
		steps = append(steps, &step)
	}

	return &wf, steps, nil
}

// UpdateWorkflowStatus modifies the status of an existing workflow.
func (s *SQLiteStore) UpdateWorkflowStatus(ctx context.Context, tenantID, id string, status domain.WorkflowStatus, errMsg *string) error {
	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	var completedStr *string
	if status.IsTerminal() {
		completedStr = &nowStr
	}

	query := `
	UPDATE workflows
	SET status = ?, error_message = ?, completed_at = COALESCE(?, completed_at)
	WHERE tenant_id = ? AND id = ? AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
	`
	res, err := s.db.ExecContext(ctx, query, string(status), errMsg, completedStr, tenantID, id)
	if err != nil {
		return err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if aff == 0 {
		var currentStatus string
		err := s.db.QueryRowContext(ctx, `SELECT status FROM workflows WHERE tenant_id = ? AND id = ?`, tenantID, id).Scan(&currentStatus)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
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
func (s *SQLiteStore) UpdateWorkflowStep(ctx context.Context, tenantID, stepID string, status domain.JobStatus, output []byte, errMsg *string) error {
	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)

	var completedStr *string
	if status.IsTerminal() {
		completedStr = &nowStr
	}

	query := `
	UPDATE workflow_steps
	SET status = ?, output_data = ?, error_message = ?, completed_at = COALESCE(?, completed_at)
	WHERE id = ? AND workflow_id IN (SELECT id FROM workflows WHERE tenant_id = ?)
	`
	res, err := s.db.ExecContext(ctx, query, string(status), string(output), errMsg, completedStr, stepID, tenantID)
	if err != nil {
		return err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if aff == 0 {
		return store.ErrNotFound
	}
	return nil
}

// GetExecutions retrieves all execution attempts for a given job.
func (s *SQLiteStore) GetExecutions(ctx context.Context, tenantID, jobID string) ([]*domain.JobExecution, error) {
	query := `
	SELECT je.id, je.job_id, je.attempt, je.fencing_generation, je.worker_id, je.status, je.error_message, je.started_at, je.finished_at, je.duration_ms
	FROM job_executions je
	JOIN jobs j ON je.job_id = j.id
	WHERE j.tenant_id = ? AND je.job_id = ?
	ORDER BY je.attempt ASC
	`
	rows, err := s.db.QueryContext(ctx, query, tenantID, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var execs []*domain.JobExecution
	for rows.Next() {
		var e domain.JobExecution
		var statusStr, startedStr string
		var errMsgSql, finishedStr sql.NullString
		var durationSql sql.NullInt64

		err := rows.Scan(
			&e.ID, &e.JobID, &e.Attempt, &e.FencingGeneration, &e.WorkerID,
			&statusStr, &errMsgSql, &startedStr, &finishedStr, &durationSql,
		)
		if err != nil {
			return nil, err
		}
		e.Status = domain.JobStatus(statusStr)
		if errMsgSql.Valid {
			e.ErrorMessage = &errMsgSql.String
		}
		e.StartedAt, _ = time.Parse(time.RFC3339Nano, startedStr)
		if finishedStr.Valid {
			t, _ := time.Parse(time.RFC3339Nano, finishedStr.String)
			e.FinishedAt = &t
		}
		if durationSql.Valid {
			e.DurationMs = &durationSql.Int64
		}
		execs = append(execs, &e)
	}
	return execs, nil
}
