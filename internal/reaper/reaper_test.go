package reaper_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/reaper"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
	"github.com/AnkitxRot/ForgeFlow/internal/store/sqlite"
	"github.com/google/uuid"
)

func setupTestStore(t *testing.T) (store.Store, string, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "forgeflow_reaper_test.db")
	s, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	tenantID := "tenant-" + uuid.New().String()[:8]
	err = s.CreateTenant(context.Background(), &domain.Tenant{
		ID:        tenantID,
		Name:      "Reaper Test Tenant",
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

func TestReaper_ExpiredLeaseDetection_RequeueAndFencing(t *testing.T) {
	s, tenantID, queueName := setupTestStore(t)
	ctx := context.Background()

	rp, err := reaper.New(reaper.Config{
		Store:     s,
		TenantID:  tenantID,
		BatchSize: 10,
		Interval:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create reaper: %v", err)
	}

	// 1. Create a job
	jobID := "job-reap-" + uuid.New().String()[:8]
	err = s.CreateJob(ctx, &domain.Job{
		ID:         jobID,
		TenantID:   tenantID,
		QueueName:  queueName,
		Status:     domain.StatusQueued,
		RunAt:      time.Now().UTC(),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	// 2. Worker A claims the job with a short lease (10ms)
	claimedA, err := s.ClaimJobs(ctx, tenantID, "worker-alpha", []string{queueName}, 1, 10*time.Millisecond)
	if err != nil || len(claimedA) != 1 {
		t.Fatalf("worker A claim failed: %v", err)
	}
	jobA := claimedA[0]
	genA := jobA.FencingGeneration
	tokenA := *jobA.LeaseToken

	// 3. Wait for lease to expire
	time.Sleep(50 * time.Millisecond)

	// 4. Run Reaper once
	reapedCount, err := rp.ReapOnce(ctx)
	if err != nil {
		t.Fatalf("reap once failed: %v", err)
	}
	if reapedCount != 1 {
		t.Fatalf("expected 1 job reaped, got %d", reapedCount)
	}

	// 5. Verify job is back in QUEUED status with incremented fencing generation
	reapedJob, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil {
		t.Fatalf("get job failed: %v", err)
	}
	if reapedJob.Status != domain.StatusQueued {
		t.Fatalf("expected status QUEUED, got %s", reapedJob.Status)
	}
	if reapedJob.WorkerID != nil || reapedJob.LeaseToken != nil {
		t.Fatalf("expected worker_id and lease_token to be cleared, got worker=%v token=%v", reapedJob.WorkerID, reapedJob.LeaseToken)
	}
	if reapedJob.FencingGeneration <= genA {
		t.Fatalf("expected fencing generation to advance: genA=%d, reapedGen=%d", genA, reapedJob.FencingGeneration)
	}

	// 6. Worker B claims the re-queued job
	claimedB, err := s.ClaimJobs(ctx, tenantID, "worker-beta", []string{queueName}, 1, 10*time.Second)
	if err != nil || len(claimedB) != 1 {
		t.Fatalf("worker B claim failed: %v", err)
	}
	jobB := claimedB[0]
	genB := jobB.FencingGeneration
	tokenB := *jobB.LeaseToken

	if genB <= genA {
		t.Fatalf("expected genB (%d) > genA (%d)", genB, genA)
	}
	if tokenB == tokenA {
		t.Fatal("expected new lease token for worker B")
	}

	// 7. Stale Worker A attempts completion with credentials A: MUST BE REJECTED with ErrLeaseLost!
	err = s.CompleteJob(ctx, tenantID, jobID, genA, tokenA, []byte(`{"stale":"data"}`))
	if err != store.ErrLeaseLost {
		t.Fatalf("expected ErrLeaseLost for stale worker A completion, got %v", err)
	}

	// 8. Active Worker B completes with credentials B: MUST SUCCEED!
	err = s.CompleteJob(ctx, tenantID, jobID, genB, tokenB, []byte(`{"valid":"result"}`))
	if err != nil {
		t.Fatalf("worker B completion failed: %v", err)
	}

	finalJob, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil || finalJob.Status != domain.StatusCompleted {
		t.Fatalf("expected final job status COMPLETED, got %s", finalJob.Status)
	}
	if string(finalJob.Result) != `{"valid":"result"}` {
		t.Fatalf("expected result from Worker B, got %s", string(finalJob.Result))
	}
}

func TestReaper_RetryExhaustion_TransitionsToTimedOut(t *testing.T) {
	s, tenantID, queueName := setupTestStore(t)
	ctx := context.Background()

	rp, err := reaper.New(reaper.Config{
		Store:     s,
		TenantID:  tenantID,
		BatchSize: 10,
	})
	if err != nil {
		t.Fatalf("failed to create reaper: %v", err)
	}

	jobID := "job-exhaust-" + uuid.New().String()[:8]
	// max_retries = 1, so the first claim (attempt 1) consumes all allowed attempts
	err = s.CreateJob(ctx, &domain.Job{
		ID:         jobID,
		TenantID:   tenantID,
		QueueName:  queueName,
		Status:     domain.StatusQueued,
		RunAt:      time.Now().UTC(),
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	// Claim job with 10ms lease
	claimed, err := s.ClaimJobs(ctx, tenantID, "worker-1", []string{queueName}, 1, 10*time.Millisecond)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim failed: %v", err)
	}

	// Wait for lease to expire
	time.Sleep(50 * time.Millisecond)

	// Run Reaper: attempt (1) >= max_retries (1) -> TIMED_OUT
	reapedCount, err := rp.ReapOnce(ctx)
	if err != nil {
		t.Fatalf("reap once failed: %v", err)
	}
	if reapedCount != 1 {
		t.Fatalf("expected 1 job reaped, got %d", reapedCount)
	}

	job, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil {
		t.Fatalf("get job failed: %v", err)
	}
	if job.Status != domain.StatusTimedOut {
		t.Fatalf("expected job status TIMED_OUT, got %s", job.Status)
	}
	if job.CompletedAt == nil {
		t.Fatal("expected completed_at to be set for TIMED_OUT job")
	}

	// Terminal state immutability: Cannot be cancelled or completed
	err = s.CancelJob(ctx, tenantID, jobID)
	if err != store.ErrTerminalState {
		t.Fatalf("expected ErrTerminalState when cancelling TIMED_OUT job, got %v", err)
	}
}

func TestReaper_UnexpiredLeasesAndIdempotency(t *testing.T) {
	s, tenantID, queueName := setupTestStore(t)
	ctx := context.Background()

	rp, _ := reaper.New(reaper.Config{
		Store:     s,
		TenantID:  tenantID,
		BatchSize: 10,
	})

	jobID := "job-active-" + uuid.New().String()[:8]
	_ = s.CreateJob(ctx, &domain.Job{
		ID:        jobID,
		TenantID:  tenantID,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		RunAt:     time.Now().UTC(),
	})

	// Claim with long lease (30s)
	_, _ = s.ClaimJobs(ctx, tenantID, "worker-active", []string{queueName}, 1, 30*time.Second)

	// Reaper pass: should reap 0 jobs
	reaped, err := rp.ReapOnce(ctx)
	if err != nil {
		t.Fatalf("reap once failed: %v", err)
	}
	if reaped != 0 {
		t.Fatalf("expected 0 jobs reaped for active unexpired lease, got %d", reaped)
	}

	// Idempotency: Second pass also reaps 0
	reaped2, err := rp.ReapOnce(ctx)
	if err != nil {
		t.Fatalf("second reap pass failed: %v", err)
	}
	if reaped2 != 0 {
		t.Fatalf("expected 0 jobs reaped on idempotent second pass, got %d", reaped2)
	}
}
