package integration_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
	"github.com/AnkitxRot/ForgeFlow/internal/store/postgres"
)

// TestPostgres_RealProcessCrash_ReaperRecovery executes Phase 7: Real Process Crash Certification.
// It starts an actual separate operating system process running a worker, allows it to claim a job,
// verifies durable ownership in PostgreSQL, terminates the process abruptly with SIGKILL/TerminateProcess,
// expires the lease, triggers the reaper, verifies durable state recovery (attempt=1, gen=2, status=QUEUED),
// allows a healthy worker to claim and complete the job, and proves that any mutation with the dead worker's
// credentials is rejected.
func TestPostgres_RealProcessCrash_ReaperRecovery(t *testing.T) {
	// Child process execution path
	if os.Getenv("TEST_REAL_PROCESS_WORKER") == "1" {
		runChildWorkerProcess()
		return
	}

	// Parent test runner path
	s := getTestPostgresStore(t)
	ctx := context.Background()

	connStr := os.Getenv("TEST_POSTGRES_URL")
	tenantID := "tenant-crash-" + uuid.NewString()[:8]
	queueName := "crash-q"
	setupTenantAndQueue(t, s, tenantID, queueName)

	jobID := "job-proc-crash-" + uuid.NewString()[:8]
	job := &domain.Job{
		ID:         jobID,
		TenantID:   tenantID,
		QueueName:  queueName,
		Status:     domain.StatusQueued,
		MaxRetries: 3,
		Payload:    []byte(`{"real_process":true}`),
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	workerID := "worker-proc-" + uuid.NewString()[:8]

	// 1. Start real child worker process
	cmd := exec.Command(os.Args[0], "-test.run=^TestPostgres_RealProcessCrash_ReaperRecovery$")
	cmd.Env = append(os.Environ(),
		"TEST_REAL_PROCESS_WORKER=1",
		"TEST_POSTGRES_URL="+connStr,
		"CRASH_TENANT_ID="+tenantID,
		"CRASH_QUEUE_NAME="+queueName,
		"CRASH_WORKER_ID="+workerID,
		"CRASH_JOB_ID="+jobID,
	)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("failed to create stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start child worker process: %v", err)
	}

	// 2. Wait for child to claim job and output credentials
	scanner := bufio.NewScanner(stdoutPipe)
	var claimedLine string
	claimedCh := make(chan string, 1)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "CLAIMED:") {
				claimedCh <- line
				return
			}
		}
	}()

	select {
	case claimedLine = <-claimedCh:
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("timeout waiting for child process to claim job")
	}

	// Format: CLAIMED:<jobID>:<gen>:<token>
	parts := strings.Split(claimedLine, ":")
	if len(parts) != 4 {
		_ = cmd.Process.Kill()
		t.Fatalf("unexpected child output: %q", claimedLine)
	}
	deadToken := parts[3]

	// 3. Verify durable ownership in PostgreSQL while worker process is alive
	dj, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("failed to query durable job: %v", err)
	}
	if dj.Status != domain.StatusRunning {
		_ = cmd.Process.Kill()
		t.Fatalf("expected status RUNNING, got %s", dj.Status)
	}
	if dj.Attempt != 1 || dj.FencingGeneration != 1 {
		_ = cmd.Process.Kill()
		t.Fatalf("expected attempt=1, gen=1; got attempt=%d, gen=%d", dj.Attempt, dj.FencingGeneration)
	}
	if dj.WorkerID == nil || *dj.WorkerID != workerID {
		_ = cmd.Process.Kill()
		t.Fatalf("expected worker_id=%s, got %v", workerID, dj.WorkerID)
	}

	// 4. Abruptly kill the worker process at OS level (SIGKILL / TerminateProcess)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("failed to kill child worker process: %v", err)
	}
	_ = cmd.Wait() // Reaped OS process

	// 5. Expire the orphaned lease in PostgreSQL
	if err := s.ExpireJobLeaseForTest(ctx, tenantID, jobID); err != nil {
		t.Fatalf("expire lease failed: %v", err)
	}

	// 6. Trigger the reaper to recover the orphaned job
	reapedCount, err := s.ReapExpiredJobs(ctx, tenantID, 10)
	if err != nil || reapedCount != 1 {
		t.Fatalf("reaper failed: count=%d, err=%v", reapedCount, err)
	}

	// 7. Verify durable recovery state: status=QUEUED, attempt=1, gen=2, lease_token=NULL
	reapedJob, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil {
		t.Fatalf("get reaped job failed: %v", err)
	}
	if reapedJob.Status != domain.StatusQueued {
		t.Fatalf("expected recovered status QUEUED, got %s", reapedJob.Status)
	}
	if reapedJob.Attempt != 1 {
		t.Fatalf("attempt must remain 1 on reap, got %d", reapedJob.Attempt)
	}
	if reapedJob.FencingGeneration != 2 {
		t.Fatalf("fencing generation must advance to 2 on reap, got %d", reapedJob.FencingGeneration)
	}
	if reapedJob.LeaseToken != nil {
		t.Fatalf("lease token must be NULL after reap, got %v", *reapedJob.LeaseToken)
	}

	// 8. Healthy second worker claims the recovered job
	healthyWorker := "worker-healthy-" + uuid.NewString()[:8]
	claimed2, err := s.ClaimJobs(ctx, tenantID, healthyWorker, []string{queueName}, 1, 30*time.Second)
	if err != nil || len(claimed2) != 1 {
		t.Fatalf("healthy worker claim failed: %v", err)
	}
	c2 := claimed2[0]
	if c2.Attempt != 2 {
		t.Fatalf("expected attempt=2 on reclaim, got %d", c2.Attempt)
	}
	if c2.FencingGeneration != 3 {
		t.Fatalf("expected generation=3 on reclaim, got %d", c2.FencingGeneration)
	}

	// Healthy worker completes job
	err = s.CompleteJob(ctx, tenantID, jobID, c2.FencingGeneration, *c2.LeaseToken, []byte(`{"recovered":true}`))
	if err != nil {
		t.Fatalf("healthy worker completion failed: %v", err)
	}

	// 9. Stale completion from dead worker MUST be rejected with ErrTerminalState or ErrLeaseLost
	err = s.CompleteJob(ctx, tenantID, jobID, 1, deadToken, []byte(`{"zombie_output":true}`))
	if !errors.Is(err, store.ErrTerminalState) && !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("expected ErrTerminalState or ErrLeaseLost for dead worker completion, got: %v", err)
	}

	// Final verification of terminal state
	finalJob, _ := s.GetJob(ctx, tenantID, jobID)
	if finalJob.Status != domain.StatusCompleted {
		t.Fatalf("expected final status COMPLETED, got %s", finalJob.Status)
	}
	if !strings.Contains(string(finalJob.Result), `"recovered"`) {
		t.Fatalf("unexpected output: %s", string(finalJob.Result))
	}
}

// runChildWorkerProcess executes inside the child OS process.
func runChildWorkerProcess() {
	connStr := os.Getenv("TEST_POSTGRES_URL")
	tenantID := os.Getenv("CRASH_TENANT_ID")
	queueName := os.Getenv("CRASH_QUEUE_NAME")
	workerID := os.Getenv("CRASH_WORKER_ID")
	targetJobID := os.Getenv("CRASH_JOB_ID")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s, err := postgres.Open(ctx, connStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child postgres open failed: %v\n", err)
		os.Exit(2)
	}
	defer s.Close()

	claimed, err := s.ClaimJobs(ctx, tenantID, workerID, []string{queueName}, 1, 10*time.Second)
	if err != nil || len(claimed) == 0 {
		fmt.Fprintf(os.Stderr, "child claim failed: %v\n", err)
		os.Exit(3)
	}

	job := claimed[0]
	if job.ID != targetJobID {
		fmt.Fprintf(os.Stderr, "child claimed unexpected job: %s\n", job.ID)
		os.Exit(4)
	}

	// Output claimed signal to parent and flush stdout
	fmt.Printf("CLAIMED:%s:%d:%s\n", job.ID, job.FencingGeneration, *job.LeaseToken)
	_ = os.Stdout.Sync()

	// Simulate in-flight work and wait to be killed by parent process
	select {}
}
