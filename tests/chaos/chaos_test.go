package chaos_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/reaper"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
	"github.com/AnkitxRot/ForgeFlow/internal/store/sqlite"
	"github.com/AnkitxRot/ForgeFlow/internal/telemetry"
	"github.com/AnkitxRot/ForgeFlow/internal/worker"
	"github.com/AnkitxRot/ForgeFlow/internal/workflow"
	"github.com/google/uuid"
)

func setupChaosStore(t *testing.T) (store.Store, string, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "forgeflow_chaos_test.db")
	s, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	tenantID := "tenant-chaos-" + uuid.New().String()[:8]
	err = s.CreateTenant(context.Background(), &domain.Tenant{
		ID:        tenantID,
		Name:      "Chaos Test Tenant",
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

// TestChaos_WorkerCrash_ReaperRecovery verifies that when a worker process crashes mid-task,
// the reaper safely recovers the orphaned lease without data corruption, allowing a healthy
// worker to claim and complete the job.
func TestChaos_WorkerCrash_ReaperRecovery(t *testing.T) {
	s, tenantID, queueName := setupChaosStore(t)
	ctx := context.Background()

	rp, err := reaper.New(reaper.Config{
		Store:     s,
		TenantID:  tenantID,
		BatchSize: 10,
	})
	if err != nil {
		t.Fatalf("failed to create reaper: %v", err)
	}

	jobID := "job-crash-" + uuid.New().String()[:8]
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

	// 1. Worker 1 claims job with short lease (20ms) and abruptly "crashes" (no heartbeat, no completion)
	claimed, err := s.ClaimJobs(ctx, tenantID, "worker-crashed", []string{queueName}, 1, 20*time.Millisecond)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim failed: %v", err)
	}
	originalGen := claimed[0].FencingGeneration

	// 2. Wait for lease to expire
	time.Sleep(60 * time.Millisecond)

	// 3. Reaper detects crashed worker's expired lease and recovers it
	reapedCount, err := rp.ReapOnce(ctx)
	if err != nil {
		t.Fatalf("reap once failed: %v", err)
	}
	if reapedCount != 1 {
		t.Fatalf("expected 1 orphaned job reaped, got %d", reapedCount)
	}

	// 4. Verify job is back in QUEUED state with advanced fencing generation
	reapedJob, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil || reapedJob.Status != domain.StatusQueued {
		t.Fatalf("expected reaped job to be QUEUED, got status=%s err=%v", reapedJob.Status, err)
	}
	if reapedJob.FencingGeneration <= originalGen {
		t.Fatalf("expected fencing generation to advance: orig=%d, now=%d", originalGen, reapedJob.FencingGeneration)
	}

	// 5. Healthy Worker 2 starts, claims the recovered job, and completes it
	registry := worker.NewHandlerRegistry()
	completedCh := make(chan struct{}, 1)
	registry.RegisterQueue(queueName, worker.HandlerFunc(func(ctx context.Context, job *domain.Job) ([]byte, error) {
		close(completedCh)
		return []byte(`{"status":"recovered_ok"}`), nil
	}))

	w2, err := worker.NewWorker(worker.Config{
		ID:           "worker-healthy",
		TenantID:     tenantID,
		Queues:       []string{queueName},
		Concurrency:  1,
		PollInterval: 20 * time.Millisecond,
		Store:        s,
		Registry:     registry,
	})
	if err != nil {
		t.Fatalf("failed to create worker 2: %v", err)
	}

	_ = w2.Start(ctx)
	defer func() { _ = w2.Stop() }()

	select {
	case <-completedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for healthy worker to execute recovered job")
	}

	time.Sleep(50 * time.Millisecond)
	finalJob, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil || finalJob.Status != domain.StatusCompleted {
		t.Fatalf("expected final job COMPLETED, got %s", finalJob.Status)
	}
}

// TestChaos_PartitionedZombie_StaleCompletionRejected verifies that late completion attempts
// from partitioned or paused workers are rejected with ErrLeaseLost and cannot corrupt newer execution state.
func TestChaos_PartitionedZombie_StaleCompletionRejected(t *testing.T) {
	s, tenantID, queueName := setupChaosStore(t)
	ctx := context.Background()

	rp, _ := reaper.New(reaper.Config{
		Store:     s,
		TenantID:  tenantID,
		BatchSize: 10,
	})

	jobID := "job-zombie-" + uuid.New().String()[:8]
	_ = s.CreateJob(ctx, &domain.Job{
		ID:         jobID,
		TenantID:   tenantID,
		QueueName:  queueName,
		Status:     domain.StatusQueued,
		RunAt:      time.Now().UTC(),
		MaxRetries: 3,
	})

	// Worker A claims with 15ms lease
	claimedA, _ := s.ClaimJobs(ctx, tenantID, "worker-a", []string{queueName}, 1, 15*time.Millisecond)
	jobA := claimedA[0]

	// Simulate GC pause or partition: lease expires
	time.Sleep(50 * time.Millisecond)

	// Reaper recovers job
	_, _ = rp.ReapOnce(ctx)

	// Worker B claims the reassigned job
	claimedB, _ := s.ClaimJobs(ctx, tenantID, "worker-b", []string{queueName}, 1, 10*time.Second)
	jobB := claimedB[0]

	// Partition heals: Worker A wakes up and attempts to complete with stale credentials
	err := s.CompleteJob(ctx, tenantID, jobID, jobA.FencingGeneration, *jobA.LeaseToken, []byte(`{"stale":"corrupted"}`))
	if !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("expected ErrLeaseLost for stale worker completion, got %v", err)
	}

	// Verify Worker B's active execution is untouched
	activeJob, _ := s.GetJob(ctx, tenantID, jobID)
	if activeJob.Status != domain.StatusRunning {
		t.Fatalf("expected job to remain RUNNING for Worker B, got %s", activeJob.Status)
	}
	if *activeJob.WorkerID != "worker-b" {
		t.Fatalf("expected worker-b to own job, got %s", *activeJob.WorkerID)
	}

	// Worker B completes successfully
	err = s.CompleteJob(ctx, tenantID, jobID, jobB.FencingGeneration, *jobB.LeaseToken, []byte(`{"valid":"clean"}`))
	if err != nil {
		t.Fatalf("worker B completion failed: %v", err)
	}

	completedJob, _ := s.GetJob(ctx, tenantID, jobID)
	if completedJob.Status != domain.StatusCompleted {
		t.Fatalf("expected job to be COMPLETED, got %s", completedJob.Status)
	}
	if string(completedJob.Result) != `{"valid":"clean"}` {
		t.Fatalf("result was corrupted by stale worker! got %s", string(completedJob.Result))
	}
}

// TestChaos_CancellationRace_TerminalImmutability verifies that a cancellation racing against
// job completion preserves the terminal state immutability invariant.
func TestChaos_CancellationRace_TerminalImmutability(t *testing.T) {
	s, tenantID, queueName := setupChaosStore(t)
	ctx := context.Background()

	jobID := "job-cancel-race-" + uuid.New().String()[:8]
	_ = s.CreateJob(ctx, &domain.Job{
		ID:        jobID,
		TenantID:  tenantID,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		RunAt:     time.Now().UTC(),
	})

	claimed, _ := s.ClaimJobs(ctx, tenantID, "worker-1", []string{queueName}, 1, 10*time.Second)
	job := claimed[0]

	// 1. External cancel arrives and commits
	err := s.CancelJob(ctx, tenantID, jobID)
	if err != nil {
		t.Fatalf("cancel job failed: %v", err)
	}

	// 2. Worker attempts completion after cancellation
	err = s.CompleteJob(ctx, tenantID, jobID, job.FencingGeneration, *job.LeaseToken, []byte(`{"result":"late"}`))
	// The job is CANCELLED (terminal), so CompleteJob must reject it with ErrTerminalState or ErrLeaseLost
	if err == nil {
		t.Fatal("expected error when completing cancelled job, got nil")
	}

	// Verify job remains in CANCELLED status
	finalJob, _ := s.GetJob(ctx, tenantID, jobID)
	if finalJob.Status != domain.StatusCancelled {
		t.Fatalf("terminal immutability violated: expected CANCELLED, got %s", finalJob.Status)
	}
}

// TestChaos_ConcurrentClaims_ZeroDuplicates verifies that under concurrent claim load
// from 20 workers racing across 10 jobs, every job is claimed exactly once without duplicates.
func TestChaos_ConcurrentClaims_ZeroDuplicates(t *testing.T) {
	s, tenantID, queueName := setupChaosStore(t)
	ctx := context.Background()

	numJobs := 10
	numWorkers := 20

	for i := 0; i < numJobs; i++ {
		jID := fmt.Sprintf("job-concurrent-%d-%s", i, uuid.New().String()[:6])
		_ = s.CreateJob(ctx, &domain.Job{
			ID:        jID,
			TenantID:  tenantID,
			QueueName: queueName,
			Status:    domain.StatusQueued,
			RunAt:     time.Now().UTC(),
		})
	}

	var claimCounts sync.Map
	var totalClaimed int64
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		workerID := fmt.Sprintf("worker-c-%d", w)
		wg.Add(1)
		go func(wID string) {
			defer wg.Done()
			for attempt := 0; attempt < 10; attempt++ {
				claimed, err := s.ClaimJobs(ctx, tenantID, wID, []string{queueName}, 2, 10*time.Second)
				if err != nil {
					continue
				}
				for _, j := range claimed {
					atomic.AddInt64(&totalClaimed, 1)
					val, _ := claimCounts.LoadOrStore(j.ID, int64(0))
					claimCounts.Store(j.ID, val.(int64)+1)
				}
				if len(claimed) == 0 {
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(workerID)
	}

	wg.Wait()

	if totalClaimed != int64(numJobs) {
		t.Fatalf("expected exactly %d total claims, got %d", numJobs, totalClaimed)
	}

	// Verify each individual job was claimed exactly 1 time
	claimCounts.Range(func(key, value any) bool {
		count := value.(int64)
		if count != 1 {
			t.Errorf("job %s was claimed %d times (expected 1)", key.(string), count)
		}
		return true
	})
}

// TestChaos_WorkflowPartialFailure verifies that failure in one branch of a workflow DAG
// cleanly propagates failure and cancels downstream/parallel unexecuted steps.
func TestChaos_WorkflowPartialFailure(t *testing.T) {
	s, tenantID, queueName := setupChaosStore(t)
	ctx := context.Background()

	engine := workflow.NewEngine(s, queueName)

	def := &workflow.Definition{
		Name: "resilient-pipeline",
		Steps: []workflow.StepDefinition{
			{Name: "step-root", Handler: "root_task"},
			{Name: "step-failing", Handler: "failing_task", Dependencies: []string{"step-root"}, FailurePolicy: domain.FailurePolicyFailWorkflow},
			{Name: "step-downstream", Handler: "downstream_task", Dependencies: []string{"step-failing"}},
		},
	}

	wf, err := engine.Submit(ctx, tenantID, def, nil)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	_, steps, _ := s.GetWorkflow(ctx, tenantID, wf.ID)
	stepMap := make(map[string]*domain.WorkflowStep)
	for _, st := range steps {
		stepMap[st.StepName] = st
	}

	// Complete root step
	_ = engine.HandleStepCompletion(ctx, tenantID, wf.ID, stepMap["step-root"].ID, []byte(`{"status":"ok"}`))

	// Fail step-failing
	err = engine.HandleStepFailure(ctx, tenantID, wf.ID, stepMap["step-failing"].ID, "unrecoverable upstream crash")
	if err != nil {
		t.Fatalf("handle step failure failed: %v", err)
	}

	finalWF, finalSteps, _ := s.GetWorkflow(ctx, tenantID, wf.ID)
	if finalWF.Status != domain.WorkflowStatusFailed {
		t.Fatalf("expected workflow status FAILED, got %s", finalWF.Status)
	}

	for _, st := range finalSteps {
		if st.StepName == "step-downstream" && st.Status != domain.StatusCancelled {
			t.Fatalf("downstream step should be CANCELLED, got %s", st.Status)
		}
	}
}

// TestChaos_ReaperRenewalRace verifies that when a lease is extended by a renewal just as
// the reaper executes, the renewed lease is NOT incorrectly reaped or reset to QUEUED.
func TestChaos_ReaperRenewalRace(t *testing.T) {
	s, tenantID, queueName := setupChaosStore(t)
	ctx := context.Background()

	rp, _ := reaper.New(reaper.Config{
		Store:     s,
		TenantID:  tenantID,
		BatchSize: 10,
	})

	jobID := "job-reap-race-" + uuid.New().String()[:8]
	_ = s.CreateJob(ctx, &domain.Job{
		ID:         jobID,
		TenantID:   tenantID,
		QueueName:  queueName,
		Status:     domain.StatusQueued,
		RunAt:      time.Now().UTC(),
		MaxRetries: 3,
	})

	// Claim with short 50ms lease
	claimed, _ := s.ClaimJobs(ctx, tenantID, "worker-race", []string{queueName}, 1, 50*time.Millisecond)
	job := claimed[0]

	// Renew lease for 10 seconds before reaper pass
	err := s.RenewLease(ctx, tenantID, jobID, job.FencingGeneration, *job.LeaseToken, 10*time.Second)
	if err != nil {
		t.Fatalf("lease renewal failed: %v", err)
	}

	// Run reaper pass
	reapedCount, err := rp.ReapOnce(ctx)
	if err != nil {
		t.Fatalf("reap once failed: %v", err)
	}
	if reapedCount != 0 {
		t.Fatalf("expected 0 jobs reaped due to active renewal, got %d", reapedCount)
	}

	// Verify job remains in RUNNING state
	currentJob, _ := s.GetJob(ctx, tenantID, jobID)
	if currentJob.Status != domain.StatusRunning {
		t.Fatalf("expected job to remain RUNNING, got %s", currentJob.Status)
	}
}

// TestChaos_ConcurrentStop_LeaseMaintainer verifies that multiple goroutines concurrently calling
// Stop on LeaseMaintainer never panic or deadlock.
func TestChaos_ConcurrentStop_LeaseMaintainer(t *testing.T) {
	s, tenantID, queueName := setupChaosStore(t)
	ctx := context.Background()

	jobID := "job-stop-race-" + uuid.New().String()[:8]
	_ = s.CreateJob(ctx, &domain.Job{
		ID:        jobID,
		TenantID:  tenantID,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		RunAt:     time.Now().UTC(),
	})

	claimed, _ := s.ClaimJobs(ctx, tenantID, "worker-stop", []string{queueName}, 1, 10*time.Second)
	job := claimed[0]

	_, cancel := context.WithCancel(ctx)
	maintainer := worker.NewLeaseMaintainer(s, job, 10*time.Second, 1*time.Second, cancel)
	maintainer.Start(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			maintainer.Stop()
		}()
	}
	wg.Wait()
}

// TestChaos_SubprocessEnvironment_SensitiveLeakPrevention verifies that sensitive variables
// such as DATABASE_URL, AWS credentials, and API keys are strictly excluded from child subprocesses
// even if mistakenly requested in AllowedEnvKeys.
func TestChaos_SubprocessEnvironment_SensitiveLeakPrevention(t *testing.T) {
	_ = os.Setenv("DATABASE_URL", "postgres://forgeflow:supersecret@localhost:5432/forgeflow")
	_ = os.Setenv("AWS_SECRET_ACCESS_KEY", "AKIAIOSFODNN7EXAMPLE")
	_ = os.Setenv("FORGEFLOW_API_KEY", "ff_live_secret_leaked")
	defer func() {
		_ = os.Unsetenv("DATABASE_URL")
		_ = os.Unsetenv("AWS_SECRET_ACCESS_KEY")
		_ = os.Unsetenv("FORGEFLOW_API_KEY")
	}()

	handler, err := worker.NewSubprocessHandler(worker.SubprocessConfig{
		BinaryPath:     "cmd.exe",
		DefaultArgs:    []string{"/c", "set"},
		AllowedEnvKeys: []string{"DATABASE_URL", "AWS_SECRET_ACCESS_KEY", "FORGEFLOW_API_KEY"},
	})
	if err != nil {
		t.Fatalf("failed to create subprocess handler: %v", err)
	}

	job := &domain.Job{
		ID:      "job-subp-leak",
		Payload: []byte(`{}`),
	}

	out, err := handler.Execute(context.Background(), job)
	if err != nil {
		t.Fatalf("subprocess execute failed: %v", err)
	}

	envDump := string(out)
	if strings.Contains(envDump, "supersecret") || strings.Contains(envDump, "DATABASE_URL") {
		t.Fatalf("CRITICAL SECURITY LEAK: DATABASE_URL leaked to subprocess!\n%s", envDump)
	}
	if strings.Contains(envDump, "AKIAIOSFODNN7EXAMPLE") || strings.Contains(envDump, "AWS_SECRET_ACCESS_KEY") {
		t.Fatalf("CRITICAL SECURITY LEAK: AWS credentials leaked to subprocess!\n%s", envDump)
	}
	if strings.Contains(envDump, "ff_live_secret_leaked") {
		t.Fatalf("CRITICAL SECURITY LEAK: API key leaked to subprocess!\n%s", envDump)
	}
}

// TestChaos_TelemetryRedaction_CompoundAndURI verifies that both compound sensitive keys
// and embedded connection string passwords in URIs are redacted by the structured logger.
func TestChaos_TelemetryRedaction_CompoundAndURI(t *testing.T) {
	var buf bytes.Buffer
	logger := telemetry.NewLogger(&buf, slog.LevelInfo)

	// Log with compound sensitive key and embedded URI password
	logger.Info("testing redaction",
		slog.String("db_password", "my-secret-password"),
		slog.String("connection_dsn", "postgres://admin:topsecret123@db.example.com:5432/forgeflow"),
		slog.String("error_detail", "failed connecting to postgresql://user:plainpass@localhost:5432/test"),
	)

	logged := buf.String()
	if strings.Contains(logged, "my-secret-password") {
		t.Fatalf("db_password was not redacted! Logged: %s", logged)
	}
	if strings.Contains(logged, "topsecret123") {
		t.Fatalf("URI password was not redacted! Logged: %s", logged)
	}
	if strings.Contains(logged, "plainpass") {
		t.Fatalf("URI plainpass was not redacted! Logged: %s", logged)
	}
	if !strings.Contains(logged, "[REDACTED]") {
		t.Fatalf("expected [REDACTED] in output! Logged: %s", logged)
	}
}
