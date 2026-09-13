package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
	"github.com/AnkitxRot/ForgeFlow/internal/store/sqlite"
)

func newTestStore(t *testing.T) *sqlite.SQLiteStore {
	t.Helper()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_forgeflow.db")

	s, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open test sqlite store: %v", err)
	}

	t.Cleanup(func() {
		_ = s.Close()
	})
	return s
}

func createTenantAndQueue(t *testing.T, s store.Store, tenantID, queueName string) {
	t.Helper()
	ctx := context.Background()

	err := s.CreateTenant(ctx, &domain.Tenant{
		ID:   tenantID,
		Name: "Tenant " + tenantID,
	})
	if err != nil && !errors.Is(err, store.ErrConflict) {
		t.Fatalf("failed to setup tenant: %v", err)
	}

	err = s.CreateQueue(ctx, &domain.Queue{
		TenantID: tenantID,
		Name:     queueName,
	})
	if err != nil && !errors.Is(err, store.ErrConflict) {
		t.Fatalf("failed to setup queue: %v", err)
	}
}

func TestSQLiteStore_TenantCRUD(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	tenant := &domain.Tenant{
		ID:   "tenant-1",
		Name: "Acme Corp",
	}

	if err := s.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}

	got, err := s.GetTenant(ctx, "tenant-1")
	if err != nil {
		t.Fatalf("failed to get tenant: %v", err)
	}
	if got.ID != "tenant-1" || got.Name != "Acme Corp" || got.Status != domain.TenantStatusActive {
		t.Errorf("unexpected tenant details: %+v", got)
	}

	// Duplicate tenant ID must return ErrConflict
	dupErr := s.CreateTenant(ctx, tenant)
	if !errors.Is(dupErr, store.ErrConflict) {
		t.Errorf("expected ErrConflict on duplicate tenant, got: %v", dupErr)
	}

	// Non-existent tenant returns ErrNotFound
	_, err = s.GetTenant(ctx, "non-existent")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestSQLiteStore_QueueCRUD(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	createTenantAndQueue(t, s, "tenant-q", "default")

	q, err := s.GetQueue(ctx, "tenant-q", "default")
	if err != nil {
		t.Fatalf("failed to get queue: %v", err)
	}
	if q.Name != "default" || q.TenantID != "tenant-q" || q.IsPaused {
		t.Errorf("unexpected queue details: %+v", q)
	}

	// Duplicate queue for same tenant returns ErrConflict
	dupErr := s.CreateQueue(ctx, &domain.Queue{TenantID: "tenant-q", Name: "default"})
	if !errors.Is(dupErr, store.ErrConflict) {
		t.Errorf("expected ErrConflict on duplicate queue, got: %v", dupErr)
	}

	// Non-existent queue returns ErrNotFound
	_, err = s.GetQueue(ctx, "tenant-q", "missing")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestSQLiteStore_TenantIsolation(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	createTenantAndQueue(t, s, "tenant-alpha", "jobs")
	createTenantAndQueue(t, s, "tenant-beta", "jobs")

	jobA := &domain.Job{
		ID:        "job-alpha-1",
		TenantID:  "tenant-alpha",
		QueueName: "jobs",
		Status:    domain.StatusQueued,
		Payload:   []byte(`{"task":"secret-alpha"}`),
	}
	if err := s.CreateJob(ctx, jobA); err != nil {
		t.Fatalf("failed to create jobA: %v", err)
	}

	// 1. Tenant Beta attempts to read Job Alpha -> MUST return ErrNotFound
	_, err := s.GetJob(ctx, "tenant-beta", "job-alpha-1")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("security violation: tenant-beta read tenant-alpha job, got err: %v", err)
	}

	// 2. Tenant Beta attempts to cancel Job Alpha -> MUST return ErrNotFound
	err = s.CancelJob(ctx, "tenant-beta", "job-alpha-1")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("security violation: tenant-beta cancelled tenant-alpha job, got err: %v", err)
	}

	// 3. Tenant Beta attempts to complete Job Alpha -> MUST return ErrNotFound
	err = s.CompleteJob(ctx, "tenant-beta", "job-alpha-1", 0, "token", []byte(`{}`))
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("security violation: tenant-beta completed tenant-alpha job, got err: %v", err)
	}
}

func TestSQLiteStore_IdempotencyConstraints(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	createTenantAndQueue(t, s, "tenant-idem-1", "default")
	createTenantAndQueue(t, s, "tenant-idem-2", "default")

	key := "checkout-event-999"

	job1 := &domain.Job{
		ID:             "job-101",
		TenantID:       "tenant-idem-1",
		QueueName:      "default",
		IdempotencyKey: &key,
		Payload:        []byte(`{"order":101}`),
	}
	if err := s.CreateJob(ctx, job1); err != nil {
		t.Fatalf("failed to create initial job: %v", err)
	}

	// Duplicate in same tenant -> ErrConflict
	job1Dup := &domain.Job{
		ID:             "job-102",
		TenantID:       "tenant-idem-1",
		QueueName:      "default",
		IdempotencyKey: &key,
		Payload:        []byte(`{"order":101}`),
	}
	err := s.CreateJob(ctx, job1Dup)
	if !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected ErrConflict for duplicate idempotency key in same tenant, got: %v", err)
	}

	// Same idempotency key in DIFFERENT tenant -> MUST succeed (tenant partition)
	jobTenant2 := &domain.Job{
		ID:             "job-201",
		TenantID:       "tenant-idem-2",
		QueueName:      "default",
		IdempotencyKey: &key,
		Payload:        []byte(`{"order":201}`),
	}
	if err := s.CreateJob(ctx, jobTenant2); err != nil {
		t.Fatalf("expected different tenant with same idempotency key to succeed, got: %v", err)
	}
}

func TestSQLiteStore_Fencing_StaleGenerationAndTokenRejection(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	createTenantAndQueue(t, s, "tenant-fence", "default")

	job := &domain.Job{
		ID:        "job-fence-1",
		TenantID:  "tenant-fence",
		QueueName: "default",
		Status:    domain.StatusQueued,
		Payload:   []byte(`{"test":true}`),
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	// Transition job to RUNNING with fencing_generation = 10, lease_token = "valid-token-10"
	validGen := int64(10)
	validToken := "valid-token-10"
	if err := s.SetJobRunningForTest("tenant-fence", "job-fence-1", validGen, validToken); err != nil {
		t.Fatalf("failed to set job running: %v", err)
	}

	// 1. Attempt CompleteJob with stale fencing_generation (9 < 10) -> ErrLeaseLost
	err := s.CompleteJob(ctx, "tenant-fence", "job-fence-1", 9, validToken, []byte(`{"out":1}`))
	if !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("expected ErrLeaseLost for stale generation, got: %v", err)
	}

	// 2. Attempt CompleteJob with wrong lease_token -> ErrLeaseLost
	err = s.CompleteJob(ctx, "tenant-fence", "job-fence-1", validGen, "wrong-token", []byte(`{"out":1}`))
	if !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("expected ErrLeaseLost for wrong lease token, got: %v", err)
	}

	// 3. Attempt RenewLease with stale generation -> ErrLeaseLost
	err = s.RenewLease(ctx, "tenant-fence", "job-fence-1", 9, validToken, 30*time.Second)
	if !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("expected ErrLeaseLost on stale RenewLease, got: %v", err)
	}

	// 4. RenewLease with valid credentials -> SUCCEEDS
	err = s.RenewLease(ctx, "tenant-fence", "job-fence-1", validGen, validToken, 30*time.Second)
	if err != nil {
		t.Fatalf("valid RenewLease failed: %v", err)
	}

	// 5. CompleteJob with valid credentials -> SUCCEEDS
	err = s.CompleteJob(ctx, "tenant-fence", "job-fence-1", validGen, validToken, []byte(`{"result":"ok"}`))
	if err != nil {
		t.Fatalf("valid CompleteJob failed: %v", err)
	}

	// Verify terminal status is COMPLETED
	completedJob, err := s.GetJob(ctx, "tenant-fence", "job-fence-1")
	if err != nil {
		t.Fatalf("get job failed: %v", err)
	}
	if completedJob.Status != domain.StatusCompleted {
		t.Fatalf("expected status COMPLETED, got: %s", completedJob.Status)
	}

	// 6. Zombie worker attempts CompleteJob again after job is completed -> ErrTerminalState
	err = s.CompleteJob(ctx, "tenant-fence", "job-fence-1", validGen, validToken, []byte(`{"late":"write"}`))
	if !errors.Is(err, store.ErrTerminalState) {
		t.Fatalf("expected ErrTerminalState when completing already completed job, got: %v", err)
	}
}

func TestSQLiteStore_FailJob_RetryBackoffAndTerminal(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	createTenantAndQueue(t, s, "tenant-fail", "default")

	job := &domain.Job{
		ID:         "job-fail-1",
		TenantID:   "tenant-fail",
		QueueName:  "default",
		Status:     domain.StatusQueued,
		MaxRetries: 3,
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	gen := int64(1)
	token := "token-fail-1"
	if err := s.SetJobRunningForTest("tenant-fail", "job-fail-1", gen, token); err != nil {
		t.Fatalf("set running failed: %v", err)
	}

	// 1. Fail with retryable = true, attempt = 1 < max_retries = 3 -> status RETRYING
	err := s.FailJob(ctx, "tenant-fail", "job-fail-1", gen, token, "transient network error", true, 10*time.Second)
	if err != nil {
		t.Fatalf("fail job failed: %v", err)
	}

	retriedJob, err := s.GetJob(ctx, "tenant-fail", "job-fail-1")
	if err != nil {
		t.Fatalf("get job failed: %v", err)
	}
	if retriedJob.Status != domain.StatusRetrying {
		t.Fatalf("expected status RETRYING, got: %s", retriedJob.Status)
	}
	if retriedJob.LeaseToken != nil {
		t.Fatalf("expected lease_token to be cleared on retry, got: %v", *retriedJob.LeaseToken)
	}
	if retriedJob.ErrorMessage == nil || *retriedJob.ErrorMessage != "transient network error" {
		t.Fatalf("unexpected error message: %v", retriedJob.ErrorMessage)
	}

	// 2. Set job running at attempt = 3 (exhausted)
	gen2 := int64(2)
	token2 := "token-fail-2"
	if err := s.SetJobRunningWithAttemptForTest("tenant-fail", "job-fail-1", gen2, token2, 3); err != nil {
		t.Fatalf("set running failed: %v", err)
	}

	// Fail with retries exhausted -> status must become FAILED (terminal)
	err = s.FailJob(ctx, "tenant-fail", "job-fail-1", gen2, token2, "fatal error", true, 10*time.Second)
	if err != nil {
		t.Fatalf("fail job exhausted failed: %v", err)
	}

	failedJob, err := s.GetJob(ctx, "tenant-fail", "job-fail-1")
	if err != nil {
		t.Fatalf("get job failed: %v", err)
	}
	if failedJob.Status != domain.StatusFailed {
		t.Fatalf("expected status FAILED, got: %s", failedJob.Status)
	}
	if failedJob.CompletedAt == nil {
		t.Fatal("expected completed_at to be set on terminal FAILED status")
	}

	// Invariant: Once in FAILED terminal state, attempting to complete/fail/cancel must return ErrTerminalState
	err = s.CompleteJob(ctx, "tenant-fail", "job-fail-1", gen2, token2, []byte(`{}`))
	if !errors.Is(err, store.ErrTerminalState) {
		t.Fatalf("expected ErrTerminalState when mutating terminal FAILED job, got: %v", err)
	}
}

func TestSQLiteStore_CancelJob_TerminalImmutability(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	createTenantAndQueue(t, s, "tenant-cancel", "default")

	job := &domain.Job{
		ID:        "job-cancel-1",
		TenantID:  "tenant-cancel",
		QueueName: "default",
		Status:    domain.StatusQueued,
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	// Cancel QUEUED job -> CANCELLED
	if err := s.CancelJob(ctx, "tenant-cancel", "job-cancel-1"); err != nil {
		t.Fatalf("cancel job failed: %v", err)
	}

	got, err := s.GetJob(ctx, "tenant-cancel", "job-cancel-1")
	if err != nil {
		t.Fatalf("get job failed: %v", err)
	}
	if got.Status != domain.StatusCancelled {
		t.Fatalf("expected status CANCELLED, got: %s", got.Status)
	}

	// Second cancel returns ErrTerminalState
	err = s.CancelJob(ctx, "tenant-cancel", "job-cancel-1")
	if !errors.Is(err, store.ErrTerminalState) {
		t.Fatalf("expected ErrTerminalState on cancelling terminal job, got: %v", err)
	}
}

func TestSQLiteStore_ExecutionHistory(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	createTenantAndQueue(t, s, "tenant-hist", "default")

	job := &domain.Job{
		ID:        "job-exec-hist",
		TenantID:  "tenant-hist",
		QueueName: "default",
		Status:    domain.StatusQueued,
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	duration := int64(245)
	now := time.Now().UTC()
	exec := &domain.JobExecution{
		ID:                "exec-1",
		JobID:             "job-exec-hist",
		Attempt:           1,
		FencingGeneration: 1,
		WorkerID:          "worker-node-1",
		Status:            domain.StatusCompleted,
		StartedAt:         now.Add(-time.Second),
		FinishedAt:        &now,
		DurationMs:        &duration,
	}

	if err := s.CreateExecution(ctx, exec); err != nil {
		t.Fatalf("failed to create execution history: %v", err)
	}

	// Duplicate execution ID returns ErrConflict
	dupErr := s.CreateExecution(ctx, exec)
	if !errors.Is(dupErr, store.ErrConflict) {
		t.Fatalf("expected ErrConflict for duplicate execution id, got: %v", dupErr)
	}

	// Tenant isolation on GetExecutions
	execs, err := s.GetExecutions(ctx, "tenant-hist", "job-exec-hist")
	if err != nil || len(execs) != 1 {
		t.Fatalf("expected 1 execution for correct tenant, got %d, err=%v", len(execs), err)
	}

	otherExecs, err := s.GetExecutions(ctx, "other-tenant", "job-exec-hist")
	if err != nil || len(otherExecs) != 0 {
		t.Fatalf("expected 0 executions for mismatched tenant, got %d, err=%v", len(otherExecs), err)
	}
}

func TestSQLiteStore_WorkflowStep_TenantIsolation(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	createTenantAndQueue(t, s, "tenant-wf-a", "default")
	createTenantAndQueue(t, s, "tenant-wf-b", "default")

	wfID := "wf-isolation-1"
	stepID := "step-isolation-1"
	now := time.Now().UTC()

	wf := &domain.Workflow{
		ID:             wfID,
		TenantID:       "tenant-wf-a",
		Name:           "Pipeline A",
		Status:         domain.WorkflowStatusRunning,
		DefinitionJSON: []byte(`{}`),
		ContextData:    []byte(`{}`),
		CreatedAt:      now,
	}
	step := &domain.WorkflowStep{
		ID:            stepID,
		WorkflowID:    wfID,
		StepName:      "step-1",
		Status:        domain.StatusPending,
		Dependencies:  []string{},
		Handler:       "handler-1",
		InputTemplate: []byte(`{}`),
		FailurePolicy: domain.FailurePolicyFailWorkflow,
		CreatedAt:     now,
	}

	if err := s.CreateWorkflow(ctx, wf, []*domain.WorkflowStep{step}); err != nil {
		t.Fatalf("create workflow failed: %v", err)
	}

	// Tenant B attempts to update Tenant A's step -> must return ErrNotFound
	err := s.UpdateWorkflowStep(ctx, "tenant-wf-b", stepID, domain.StatusCompleted, []byte(`{"spoofed":true}`), nil)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound when Tenant B mutates Tenant A's step, got: %v", err)
	}

	// Tenant A updates step -> succeeds
	err = s.UpdateWorkflowStep(ctx, "tenant-wf-a", stepID, domain.StatusCompleted, []byte(`{"valid":true}`), nil)
	if err != nil {
		t.Fatalf("expected Tenant A to update step successfully, got: %v", err)
	}
}

func TestSQLiteStore_WorkflowTerminalImmutability(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	createTenantAndQueue(t, s, "tenant-term-wf", "default")

	wfID := "wf-term-1"
	now := time.Now().UTC()

	wf := &domain.Workflow{
		ID:             wfID,
		TenantID:       "tenant-term-wf",
		Name:           "Terminal WF",
		Status:         domain.WorkflowStatusRunning,
		DefinitionJSON: []byte(`{}`),
		ContextData:    []byte(`{}`),
		CreatedAt:      now,
	}
	step := &domain.WorkflowStep{
		ID:            "step-term-1",
		WorkflowID:    wfID,
		StepName:      "step-1",
		Status:        domain.StatusPending,
		Dependencies:  []string{},
		Handler:       "handler-1",
		InputTemplate: []byte(`{}`),
		FailurePolicy: domain.FailurePolicyFailWorkflow,
		CreatedAt:     now,
	}

	if err := s.CreateWorkflow(ctx, wf, []*domain.WorkflowStep{step}); err != nil {
		t.Fatalf("create workflow failed: %v", err)
	}

	// Complete workflow
	if err := s.UpdateWorkflowStatus(ctx, "tenant-term-wf", wfID, domain.WorkflowStatusCompleted, nil); err != nil {
		t.Fatalf("complete workflow failed: %v", err)
	}

	// Attempting to mutate completed workflow must return ErrTerminalState
	err := s.UpdateWorkflowStatus(ctx, "tenant-term-wf", wfID, domain.WorkflowStatusFailed, nil)
	if !errors.Is(err, store.ErrTerminalState) {
		t.Fatalf("expected ErrTerminalState on mutating completed workflow, got: %v", err)
	}
}
