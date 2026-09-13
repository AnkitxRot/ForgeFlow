package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
	"github.com/AnkitxRot/ForgeFlow/internal/store/postgres"
)

func getTestPostgresStore(t *testing.T) *postgres.PostgresStore {
	t.Helper()
	connStr := os.Getenv("TEST_POSTGRES_URL")
	if connStr == "" {
		t.Skip("skipping PostgreSQL integration test: TEST_POSTGRES_URL environment variable is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s, err := postgres.Open(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to open postgres test store: %v", err)
	}

	t.Cleanup(func() {
		_ = s.Close()
	})
	return s
}

func setupTenantAndQueue(t *testing.T, s *postgres.PostgresStore, tenantID, queueName string) {
	t.Helper()
	ctx := context.Background()

	err := s.CreateTenant(ctx, &domain.Tenant{
		ID:   tenantID,
		Name: "Integration " + tenantID,
	})
	if err != nil && !errors.Is(err, store.ErrConflict) {
		t.Fatalf("setup tenant failed: %v", err)
	}

	err = s.CreateQueue(ctx, &domain.Queue{
		TenantID: tenantID,
		Name:     queueName,
	})
	if err != nil && !errors.Is(err, store.ErrConflict) {
		t.Fatalf("setup queue failed: %v", err)
	}
}

// TestPostgres_50WorkersSingleJobClaim proves that with 50 concurrent worker transactions
// contending for 1 eligible job, exactly 1 claim succeeds and zero duplicate claims occur.
func TestPostgres_50WorkersSingleJobClaim(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantID := "tenant-claim-" + uuid.NewString()[:8]
	queueName := "critical-tasks"
	setupTenantAndQueue(t, s, tenantID, queueName)

	jobID := "job-solo-" + uuid.NewString()[:8]
	job := &domain.Job{
		ID:        jobID,
		TenantID:  tenantID,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		Priority:  10,
		Payload:   []byte(`{"task":"exclusive-computation"}`),
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	concurrency := 50
	var wg sync.WaitGroup
	wg.Add(concurrency)

	type claimResult struct {
		workerID string
		jobs     []*domain.Job
		err      error
	}
	results := make(chan claimResult, concurrency)

	// Barrier to ensure all 50 workers start claiming simultaneously
	startBarrier := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		workerID := fmt.Sprintf("worker-node-%02d", i)
		go func(wID string) {
			defer wg.Done()
			<-startBarrier

			claimCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			claimed, err := s.ClaimJobs(claimCtx, tenantID, wID, []string{queueName}, 1, 30*time.Second)
			results <- claimResult{workerID: wID, jobs: claimed, err: err}
		}(workerID)
	}

	close(startBarrier)
	wg.Wait()
	close(results)

	var successfulClaims int
	var winningWorkerID string
	var winningJob *domain.Job

	for res := range results {
		if res.err != nil {
			t.Errorf("worker %s claim returned unexpected error: %v", res.workerID, res.err)
			continue
		}
		if len(res.jobs) > 0 {
			successfulClaims++
			winningWorkerID = res.workerID
			winningJob = res.jobs[0]
		}
	}

	// Invariant: Exactly 1 worker must claim the single available job
	if successfulClaims != 1 {
		t.Fatalf("concurrency invariant violated: expected exactly 1 successful claim, got %d", successfulClaims)
	}

	// Verify winning job ownership fields are consistent
	if winningJob.ID != jobID {
		t.Errorf("expected claimed job ID %s, got %s", jobID, winningJob.ID)
	}
	if winningJob.WorkerID == nil || *winningJob.WorkerID != winningWorkerID {
		t.Errorf("expected worker_id %s, got %v", winningWorkerID, winningJob.WorkerID)
	}
	if winningJob.FencingGeneration != 1 {
		t.Errorf("expected fencing_generation 1, got %d", winningJob.FencingGeneration)
	}
	if winningJob.Attempt != 1 {
		t.Errorf("expected attempt 1, got %d", winningJob.Attempt)
	}
	if winningJob.Status != domain.StatusRunning {
		t.Errorf("expected status RUNNING, got %s", winningJob.Status)
	}
	if winningJob.LeaseToken == nil || *winningJob.LeaseToken == "" {
		t.Error("expected non-empty lease_token on claimed job")
	}
	if winningJob.LeaseExpiresAt == nil || winningJob.LeaseExpiresAt.Before(time.Now().UTC()) {
		t.Error("expected future lease_expires_at on claimed job")
	}
}

// TestPostgres_ZombieFencing_Reassignment proves that if Worker A's lease expires
// and Worker B subsequently claims the job (advancing fencing_generation), Worker A's
// late completion attempt is rejected with ErrLeaseLost and Worker B's execution remains intact.
func TestPostgres_ZombieFencing_Reassignment(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantID := "tenant-zombie-" + uuid.NewString()[:8]
	queueName := "jobs"
	setupTenantAndQueue(t, s, tenantID, queueName)

	jobID := "job-zombie-" + uuid.NewString()[:8]
	job := &domain.Job{
		ID:        jobID,
		TenantID:  tenantID,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		Payload:   []byte(`{"process":"video-transcode"}`),
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	// Step 1: Worker A claims the job
	workerA := "worker-alpha"
	claimedA, err := s.ClaimJobs(ctx, tenantID, workerA, []string{queueName}, 1, 10*time.Second)
	if err != nil || len(claimedA) != 1 {
		t.Fatalf("worker A claim failed: %v, claimed count: %d", err, len(claimedA))
	}
	jobA := claimedA[0]
	genA := jobA.FencingGeneration
	tokenA := *jobA.LeaseToken

	if genA != 1 {
		t.Fatalf("expected initial generation 1, got %d", genA)
	}

	// Step 2: Simulate lease expiration and requeue by test harness / reaper
	if err := s.RequeueExpiredJob(ctx, tenantID, jobID); err != nil {
		t.Fatalf("requeue failed: %v", err)
	}

	// Step 3: Worker B claims the requeued job
	workerB := "worker-beta"
	claimedB, err := s.ClaimJobs(ctx, tenantID, workerB, []string{queueName}, 1, 30*time.Second)
	if err != nil || len(claimedB) != 1 {
		t.Fatalf("worker B claim failed: %v, claimed count: %d", err, len(claimedB))
	}
	jobB := claimedB[0]
	genB := jobB.FencingGeneration
	tokenB := *jobB.LeaseToken

	if genB <= genA {
		t.Fatalf("fencing generation invariant violated: expected genB (%d) > genA (%d)", genB, genA)
	}
	if tokenB == tokenA {
		t.Fatalf("lease token invariant violated: expected new lease token for reassigned job")
	}

	// Step 4: Stale Worker A wakes up and attempts to CompleteJob using generation A / token A
	err = s.CompleteJob(ctx, tenantID, jobID, genA, tokenA, []byte(`{"result":"stale-output"}`))
	if !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("critical fencing violation: stale worker A completion was NOT rejected with ErrLeaseLost, got: %v", err)
	}

	// Step 5: Verify Job remains in RUNNING status, owned by Worker B
	activeJob, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil {
		t.Fatalf("get job failed: %v", err)
	}
	if activeJob.Status != domain.StatusRunning {
		t.Fatalf("expected job to remain RUNNING, got: %s", activeJob.Status)
	}
	if activeJob.WorkerID == nil || *activeJob.WorkerID != workerB {
		t.Fatalf("expected worker B (%s) to retain ownership, got: %v", workerB, activeJob.WorkerID)
	}

	// Step 6: Worker B completes the job successfully using generation B / token B
	err = s.CompleteJob(ctx, tenantID, jobID, genB, tokenB, []byte(`{"result":"valid-output"}`))
	if err != nil {
		t.Fatalf("worker B valid completion failed: %v", err)
	}

	// Step 7: Verify final status is COMPLETED
	finalJob, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil {
		t.Fatalf("get final job failed: %v", err)
	}
	if finalJob.Status != domain.StatusCompleted {
		t.Fatalf("expected status COMPLETED, got: %s", finalJob.Status)
	}

	// Step 8: Subsequent mutation on terminal job must return ErrTerminalState
	err = s.CompleteJob(ctx, tenantID, jobID, genB, tokenB, []byte(`{"late":"again"}`))
	if !errors.Is(err, store.ErrTerminalState) {
		t.Fatalf("expected ErrTerminalState when mutating completed job, got: %v", err)
	}
}

// TestPostgres_PriorityAndScheduleOrdering proves that:
// 1. High-priority jobs are claimed before lower-priority jobs.
// 2. Future jobs (run_at > NOW()) are never claimed prematurely.
func TestPostgres_PriorityAndScheduleOrdering(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantID := "tenant-order-" + uuid.NewString()[:8]
	queueName := "priority-queue"
	setupTenantAndQueue(t, s, tenantID, queueName)

	now := time.Now().UTC()

	// 1. Low priority (priority = 1)
	jobLow := &domain.Job{
		ID:        "job-low-" + uuid.NewString()[:8],
		TenantID:  tenantID,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		Priority:  1,
		RunAt:     now.Add(-time.Minute),
	}
	// 2. High priority (priority = 100)
	jobHigh := &domain.Job{
		ID:        "job-high-" + uuid.NewString()[:8],
		TenantID:  tenantID,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		Priority:  100,
		RunAt:     now.Add(-time.Minute),
	}
	// 3. Future job (priority = 1000, but run_at is in the future)
	jobFuture := &domain.Job{
		ID:        "job-future-" + uuid.NewString()[:8],
		TenantID:  tenantID,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		Priority:  1000,
		RunAt:     now.Add(2 * time.Hour),
	}

	if err := s.CreateJob(ctx, jobLow); err != nil {
		t.Fatalf("create low failed: %v", err)
	}
	if err := s.CreateJob(ctx, jobHigh); err != nil {
		t.Fatalf("create high failed: %v", err)
	}
	if err := s.CreateJob(ctx, jobFuture); err != nil {
		t.Fatalf("create future failed: %v", err)
	}

	// Claim 1 job: High priority job must be claimed first
	claimed1, err := s.ClaimJobs(ctx, tenantID, "worker-1", []string{queueName}, 1, 30*time.Second)
	if err != nil || len(claimed1) != 1 {
		t.Fatalf("first claim failed: %v, count: %d", err, len(claimed1))
	}
	if claimed1[0].ID != jobHigh.ID {
		t.Fatalf("priority ordering failed: expected job %s (priority 100), got %s (priority %d)",
			jobHigh.ID, claimed1[0].ID, claimed1[0].Priority)
	}

	// Claim 2nd job: Low priority job must be claimed next
	claimed2, err := s.ClaimJobs(ctx, tenantID, "worker-1", []string{queueName}, 1, 30*time.Second)
	if err != nil || len(claimed2) != 1 {
		t.Fatalf("second claim failed: %v, count: %d", err, len(claimed2))
	}
	if claimed2[0].ID != jobLow.ID {
		t.Fatalf("expected job %s (priority 1), got %s", jobLow.ID, claimed2[0].ID)
	}

	// Claim 3rd job: Future job must NOT be claimed
	claimed3, err := s.ClaimJobs(ctx, tenantID, "worker-1", []string{queueName}, 1, 30*time.Second)
	if err != nil {
		t.Fatalf("third claim failed: %v", err)
	}
	if len(claimed3) != 0 {
		t.Fatalf("scheduling invariant violated: claimed future job prematurely: %+v", claimed3[0])
	}
}

// TestPostgres_TenantIsolation proves that a worker claiming tasks for Tenant A
// cannot claim or observe tasks belonging to Tenant B.
func TestPostgres_TenantIsolation(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantA := "tenant-a-" + uuid.NewString()[:8]
	tenantB := "tenant-b-" + uuid.NewString()[:8]
	queueName := "shared-queue-name"

	setupTenantAndQueue(t, s, tenantA, queueName)
	setupTenantAndQueue(t, s, tenantB, queueName)

	jobA := &domain.Job{
		ID:        "job-a-" + uuid.NewString()[:8],
		TenantID:  tenantA,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		Payload:   []byte(`{"tenant":"A"}`),
	}
	if err := s.CreateJob(ctx, jobA); err != nil {
		t.Fatalf("create job A failed: %v", err)
	}

	// Worker querying for Tenant B must receive 0 jobs
	claimedB, err := s.ClaimJobs(ctx, tenantB, "worker-b", []string{queueName}, 10, 30*time.Second)
	if err != nil {
		t.Fatalf("claim for tenant B failed: %v", err)
	}
	if len(claimedB) != 0 {
		t.Fatalf("tenant isolation violation: tenant B claimed tenant A job: %+v", claimedB[0])
	}
}
