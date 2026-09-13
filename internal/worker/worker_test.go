package worker_test

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
	"github.com/AnkitxRot/ForgeFlow/internal/store/sqlite"
	"github.com/AnkitxRot/ForgeFlow/internal/worker"
	"github.com/google/uuid"
)

func setupTestStore(t *testing.T) (store.Store, string, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "forgeflow_worker_test.db")
	s, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to create sqlite store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	tenantID := "tenant-" + uuid.New().String()[:8]
	err = s.CreateTenant(context.Background(), &domain.Tenant{
		ID:        tenantID,
		Name:      "Worker Test Tenant",
		Status:    "ACTIVE",
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}

	queueName := "default"
	err = s.CreateQueue(context.Background(), &domain.Queue{
		TenantID:  tenantID,
		Name:      queueName,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	return s, tenantID, queueName
}

func TestWorker_SuccessfulJobExecution(t *testing.T) {
	s, tenantID, queueName := setupTestStore(t)
	ctx := context.Background()

	registry := worker.NewHandlerRegistry()
	executed := make(chan struct{}, 1)
	registry.RegisterQueue(queueName, worker.HandlerFunc(func(ctx context.Context, job *domain.Job) ([]byte, error) {
		close(executed)
		return []byte(`{"status":"ok"}`), nil
	}))

	w, err := worker.NewWorker(worker.Config{
		ID:           "worker-1",
		TenantID:     tenantID,
		Queues:       []string{queueName},
		Concurrency:  2,
		PollInterval: 20 * time.Millisecond,
		Store:        s,
		Registry:     registry,
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	jobID := "job-" + uuid.New().String()[:8]
	err = s.CreateJob(ctx, &domain.Job{
		ID:        jobID,
		TenantID:  tenantID,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		RunAt:     time.Now().UTC(),
		Payload:   []byte(`{"action":"compute"}`),
	})
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	if err := w.Start(ctx); err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}
	defer func() { _ = w.Stop() }()

	select {
	case <-executed:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for job execution")
	}

	// Verify job transitioned to COMPLETED in store
	var job *domain.Job
	for i := 0; i < 20; i++ {
		job, err = s.GetJob(ctx, tenantID, jobID)
		if err == nil && job.Status == domain.StatusCompleted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if job.Status != domain.StatusCompleted {
		t.Fatalf("expected job status COMPLETED, got %s", job.Status)
	}
	if string(job.Result) != `{"status":"ok"}` {
		t.Fatalf("expected result %q, got %q", `{"status":"ok"}`, string(job.Result))
	}
}

func TestWorker_FailedJob_RetryableAndNonRetryable(t *testing.T) {
	s, tenantID, queueName := setupTestStore(t)
	ctx := context.Background()

	registry := worker.NewHandlerRegistry()

	// 1. Retryable failure handler
	registry.RegisterType("retryable_task", worker.HandlerFunc(func(ctx context.Context, job *domain.Job) ([]byte, error) {
		return nil, errors.New("transient network failure")
	}))

	// 2. Non-retryable failure handler
	registry.RegisterType("terminal_task", worker.HandlerFunc(func(ctx context.Context, job *domain.Job) ([]byte, error) {
		return nil, worker.MarkNonRetryable(errors.New("invalid payload structure"))
	}))

	w, err := worker.NewWorker(worker.Config{
		ID:           "worker-1",
		TenantID:     tenantID,
		Queues:       []string{queueName},
		Concurrency:  2,
		PollInterval: 20 * time.Millisecond,
		Store:        s,
		Registry:     registry,
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	// Submit retryable job
	retryJobID := "job-retry-" + uuid.New().String()[:8]
	_ = s.CreateJob(ctx, &domain.Job{
		ID:                  retryJobID,
		TenantID:            tenantID,
		QueueName:           queueName,
		Status:              domain.StatusQueued,
		RunAt:               time.Now().UTC(),
		MaxRetries:          3,
		RetryBackoffSeconds: 1,
		Payload:             []byte(`{"type":"retryable_task"}`),
	})

	// Submit non-retryable job
	terminalJobID := "job-terminal-" + uuid.New().String()[:8]
	_ = s.CreateJob(ctx, &domain.Job{
		ID:                  terminalJobID,
		TenantID:            tenantID,
		QueueName:           queueName,
		Status:              domain.StatusQueued,
		RunAt:               time.Now().UTC(),
		MaxRetries:          3,
		RetryBackoffSeconds: 1,
		Payload:             []byte(`{"type":"terminal_task"}`),
	})

	if err := w.Start(ctx); err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}
	defer func() { _ = w.Stop() }()

	// Wait and verify retryable job enters RETRYING status
	var retryJob *domain.Job
	for i := 0; i < 30; i++ {
		retryJob, err = s.GetJob(ctx, tenantID, retryJobID)
		if err == nil && retryJob.Status == domain.StatusRetrying {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if retryJob == nil || retryJob.Status != domain.StatusRetrying {
		t.Fatalf("expected retryable job status RETRYING, got %v", retryJob)
	}

	// Wait and verify non-retryable job enters FAILED status directly
	var terminalJob *domain.Job
	for i := 0; i < 30; i++ {
		terminalJob, err = s.GetJob(ctx, tenantID, terminalJobID)
		if err == nil && terminalJob.Status == domain.StatusFailed {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if terminalJob == nil || terminalJob.Status != domain.StatusFailed {
		t.Fatalf("expected terminal job status FAILED, got %v", terminalJob)
	}
}

func TestWorker_HeartbeatRenewsLease(t *testing.T) {
	s, tenantID, queueName := setupTestStore(t)
	ctx := context.Background()

	registry := worker.NewHandlerRegistry()
	renewChecked := make(chan struct{}, 1)

	// Job runs longer than original lease duration, relying on heartbeat renewal
	registry.RegisterQueue(queueName, worker.HandlerFunc(func(ctx context.Context, job *domain.Job) ([]byte, error) {
		// Wait long enough for heartbeats to tick
		time.Sleep(600 * time.Millisecond)
		close(renewChecked)
		return []byte(`{"status":"done"}`), nil
	}))

	w, err := worker.NewWorker(worker.Config{
		ID:                "worker-heartbeat",
		TenantID:          tenantID,
		Queues:            []string{queueName},
		Concurrency:       1,
		PollInterval:      20 * time.Millisecond,
		LeaseDuration:     400 * time.Millisecond,
		HeartbeatInterval: 120 * time.Millisecond,
		Store:             s,
		Registry:          registry,
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	jobID := "job-hb-" + uuid.New().String()[:8]
	_ = s.CreateJob(ctx, &domain.Job{
		ID:        jobID,
		TenantID:  tenantID,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		RunAt:     time.Now().UTC(),
	})

	_ = w.Start(ctx)
	defer func() { _ = w.Stop() }()

	select {
	case <-renewChecked:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for long-running job execution")
	}

	// Verify job successfully completed despite running longer than initial lease duration
	var job *domain.Job
	for i := 0; i < 20; i++ {
		job, err = s.GetJob(ctx, tenantID, jobID)
		if err == nil && job.Status == domain.StatusCompleted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if job.Status != domain.StatusCompleted {
		t.Fatalf("expected job to be COMPLETED after successful renewals, got %s", job.Status)
	}
}

func TestWorker_ExpiredLeaseCausesCancellation(t *testing.T) {
	s, tenantID, queueName := setupTestStore(t)
	ctx := context.Background()

	cancelledCh := make(chan struct{}, 1)
	registry := worker.NewHandlerRegistry()

	jobID := "job-lost-lease-" + uuid.New().String()[:8]

	registry.RegisterQueue(queueName, worker.HandlerFunc(func(ctx context.Context, job *domain.Job) ([]byte, error) {
		// As soon as handler runs, simulate external lease theft by advancing generation in store
		_ = s.RenewLease(context.Background(), tenantID, jobID, job.FencingGeneration, *job.LeaseToken, 10*time.Second)

		// Tamper store: change fencing_generation manually via direct mutation to simulate reaper reassignment
		// We reassign lease to another worker ID/token
		fakeToken := uuid.New().String()
		_ = s.RenewLease(context.Background(), tenantID, jobID, job.FencingGeneration, fakeToken, 10*time.Second)

		// Wait for context cancellation triggered by heartbeat failure
		select {
		case <-ctx.Done():
			close(cancelledCh)
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
			return nil, errors.New("timeout waiting for cancellation")
		}
	}))

	w, err := worker.NewWorker(worker.Config{
		ID:                "worker-lease-lost",
		TenantID:          tenantID,
		Queues:            []string{queueName},
		Concurrency:       1,
		PollInterval:      20 * time.Millisecond,
		LeaseDuration:     300 * time.Millisecond,
		HeartbeatInterval: 80 * time.Millisecond,
		Store:             s,
		Registry:          registry,
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	_ = s.CreateJob(ctx, &domain.Job{
		ID:        jobID,
		TenantID:  tenantID,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		RunAt:     time.Now().UTC(),
	})

	_ = w.Start(ctx)
	defer func() { _ = w.Stop() }()

	// Force heartbeat failure: mutate lease in store behind worker's back
	time.Sleep(100 * time.Millisecond)
	// Cancel the lease in the store
	_ = s.CancelJob(ctx, tenantID, jobID)

	select {
	case <-cancelledCh:
		// Succeeded: execution received context cancellation when heartbeat detected lease lost
	case <-time.After(3 * time.Second):
		t.Fatal("expected execution context to be cancelled after lease lost")
	}
}

func TestWorker_GracefulShutdown_DrainsInFlightWork(t *testing.T) {
	s, tenantID, queueName := setupTestStore(t)
	ctx := context.Background()

	started := make(chan struct{})
	finishJob := make(chan struct{})

	registry := worker.NewHandlerRegistry()
	registry.RegisterQueue(queueName, worker.HandlerFunc(func(ctx context.Context, job *domain.Job) ([]byte, error) {
		close(started)
		<-finishJob
		return []byte(`{"drain":"success"}`), nil
	}))

	w, err := worker.NewWorker(worker.Config{
		ID:           "worker-drain",
		TenantID:     tenantID,
		Queues:       []string{queueName},
		Concurrency:  2,
		PollInterval: 20 * time.Millisecond,
		DrainTimeout: 2 * time.Second,
		Store:        s,
		Registry:     registry,
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	jobID := "job-drain-" + uuid.New().String()[:8]
	_ = s.CreateJob(ctx, &domain.Job{
		ID:        jobID,
		TenantID:  tenantID,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		RunAt:     time.Now().UTC(),
	})

	_ = w.Start(ctx)

	// Wait for job to start
	<-started
	if w.ActiveJobs() != 1 {
		t.Fatalf("expected 1 active job, got %d", w.ActiveJobs())
	}

	// Trigger Drain asynchronously
	drainComplete := make(chan error, 1)
	go func() {
		drainComplete <- w.Drain(context.Background())
	}()

	// Enqueue a second job AFTER drain started; it must NOT be claimed
	secondJobID := "job-drain-second-" + uuid.New().String()[:8]
	_ = s.CreateJob(ctx, &domain.Job{
		ID:        secondJobID,
		TenantID:  tenantID,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		RunAt:     time.Now().UTC(),
	})

	time.Sleep(50 * time.Millisecond)
	if w.State() != worker.StateDraining && w.State() != worker.StateStopped {
		t.Fatalf("expected worker state DRAINING or STOPPED, got %s", w.State())
	}

	// Allow the first in-flight job to finish
	close(finishJob)

	// Drain should complete without error
	select {
	case err := <-drainComplete:
		if err != nil {
			t.Fatalf("drain returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("drain timed out")
	}

	if w.State() != worker.StateStopped {
		t.Fatalf("expected final state STOPPED, got %s", w.State())
	}

	// Verify first job completed
	job1, _ := s.GetJob(ctx, tenantID, jobID)
	if job1.Status != domain.StatusCompleted {
		t.Fatalf("expected in-flight job to reach COMPLETED, got %s", job1.Status)
	}

	// Verify second job was NOT claimed (remains QUEUED)
	job2, _ := s.GetJob(ctx, tenantID, secondJobID)
	if job2.Status != domain.StatusQueued {
		t.Fatalf("expected second job to remain QUEUED, got %s", job2.Status)
	}
}

func TestSubprocessHandler_ExecutionAndCancellation(t *testing.T) {
	// Look for standard go binary
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go binary not found on path, skipping subprocess test")
	}

	handler, err := worker.NewSubprocessHandler(worker.SubprocessConfig{
		BinaryPath:  goBin,
		DefaultArgs: []string{"version"},
	})
	if err != nil {
		t.Fatalf("failed to create subprocess handler: %v", err)
	}

	job := &domain.Job{
		ID:        "job-subp",
		TenantID:  "tenant-subp",
		QueueName: "default",
	}

	// 1. Test normal execution
	output, err := handler.Execute(context.Background(), job)
	if err != nil {
		t.Fatalf("subprocess execution failed: %v", err)
	}
	if len(output) == 0 {
		t.Fatal("expected non-empty output from 'go version'")
	}

	// 2. Test cancellation
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	_, err = handler.Execute(cancelCtx, job)
	if err == nil {
		t.Fatal("expected error on pre-cancelled subprocess execution")
	}
}
