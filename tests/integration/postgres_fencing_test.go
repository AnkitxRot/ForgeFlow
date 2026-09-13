package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
)

// TestPostgres_DeepFencingStress_CascadeAndConcurrent tests multi-generation cascading
// ownership transfers (A -> B -> C -> D) and verifies that every stale worker attempt
// (Complete, Fail, Renew) across past generations is rejected with ErrLeaseLost,
// both sequentially and under concurrent bombardment.
func TestPostgres_DeepFencingStress_CascadeAndConcurrent(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantID := "tenant-fence-" + uuid.NewString()[:8]
	queueName := "fencing-queue"
	setupTenantAndQueue(t, s, tenantID, queueName)

	jobID := "job-fencing-" + uuid.NewString()[:8]
	job := &domain.Job{
		ID:        jobID,
		TenantID:  tenantID,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		Payload:   []byte(`{"fencing":true}`),
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	type workerCreds struct {
		workerID string
		gen      int64
		token    string
	}

	creds := make([]workerCreds, 4)
	workerNames := []string{"worker-A", "worker-B", "worker-C", "worker-D"}

	for i, name := range workerNames {
		claimed, err := s.ClaimJobs(ctx, tenantID, name, []string{queueName}, 1, 30*time.Second)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("step %d (%s) claim failed: %v, count: %d", i, name, err, len(claimed))
		}
		cJob := claimed[0]
		creds[i] = workerCreds{
			workerID: name,
			gen:      cJob.FencingGeneration,
			token:    *cJob.LeaseToken,
		}

		// Verify monotonic generation increment
		if i > 0 && creds[i].gen <= creds[i-1].gen {
			t.Fatalf("generation non-monotonic: creds[%d].gen=%d, creds[%d].gen=%d",
				i, creds[i].gen, i-1, creds[i-1].gen)
		}

		// Check stale credentials from all prior workers
		for prev := 0; prev < i; prev++ {
			stale := creds[prev]

			// CompleteJob with stale creds must fail
			err = s.CompleteJob(ctx, tenantID, jobID, stale.gen, stale.token, []byte(`{"stale":true}`))
			if !errors.Is(err, store.ErrLeaseLost) {
				t.Fatalf("stale CompleteJob (%s gen %d) must return ErrLeaseLost, got: %v", stale.workerID, stale.gen, err)
			}

			// FailJob with stale creds must fail
			err = s.FailJob(ctx, tenantID, jobID, stale.gen, stale.token, "stale fail", false, 0)
			if !errors.Is(err, store.ErrLeaseLost) {
				t.Fatalf("stale FailJob (%s gen %d) must return ErrLeaseLost, got: %v", stale.workerID, stale.gen, err)
			}

			// RenewLease with stale creds must fail
			err = s.RenewLease(ctx, tenantID, jobID, stale.gen, stale.token, 10*time.Second)
			if !errors.Is(err, store.ErrLeaseLost) {
				t.Fatalf("stale RenewLease (%s gen %d) must return ErrLeaseLost, got: %v", stale.workerID, stale.gen, err)
			}
		}

		// If not the last worker, simulate lease expiry and requeue
		if i < len(workerNames)-1 {
			if err := s.RequeueExpiredJob(ctx, tenantID, jobID); err != nil {
				t.Fatalf("requeue failed at step %d: %v", i, err)
			}
		}
	}

	// At this point, Worker D is the active owner (gen 4)
	activeCreds := creds[3]

	// Concurrent bombardment: 30 goroutines representing stale workers A, B, C
	// repeatedly attack CompleteJob, FailJob, and RenewLease concurrently
	var wg sync.WaitGroup
	concurrentAttackers := 30
	wg.Add(concurrentAttackers)
	var staleRejected atomic.Int64

	for a := 0; a < concurrentAttackers; a++ {
		staleIndex := a % 3 // Workers A, B, or C
		stale := creds[staleIndex]
		go func(sc workerCreds, iter int) {
			defer wg.Done()
			var err error
			switch iter % 3 {
			case 0:
				err = s.CompleteJob(ctx, tenantID, jobID, sc.gen, sc.token, []byte(`{"bombard":true}`))
			case 1:
				err = s.FailJob(ctx, tenantID, jobID, sc.gen, sc.token, "bombard fail", false, 0)
			case 2:
				err = s.RenewLease(ctx, tenantID, jobID, sc.gen, sc.token, 5*time.Second)
			}
			if errors.Is(err, store.ErrLeaseLost) {
				staleRejected.Add(1)
			}
		}(stale, a)
	}

	wg.Wait()
	if int(staleRejected.Load()) != concurrentAttackers {
		t.Fatalf("expected all %d concurrent stale mutations to be rejected with ErrLeaseLost, got %d",
			concurrentAttackers, staleRejected.Load())
	}

	// Active worker D completes the job
	err := s.CompleteJob(ctx, tenantID, jobID, activeCreds.gen, activeCreds.token, []byte(`{"final":"valid"}`))
	if err != nil {
		t.Fatalf("valid completion by active worker D failed: %v", err)
	}

	// Verify terminal state
	finalJob, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil || finalJob.Status != domain.StatusCompleted {
		t.Fatalf("expected status COMPLETED, got %v (err=%v)", finalJob.Status, err)
	}

	// Any subsequent mutation must return ErrTerminalState
	err = s.CancelJob(ctx, tenantID, jobID)
	if !errors.Is(err, store.ErrTerminalState) {
		t.Fatalf("expected ErrTerminalState on cancelling completed job, got: %v", err)
	}
}

// TestPostgres_AttemptSemantics_LifecycleTrace verifies attempt progression:
// creation (attempt=0) -> claim (attempt=1) -> retryable failure -> reclaim (attempt=2) -> max retries exhausted -> FAILED
func TestPostgres_AttemptSemantics_LifecycleTrace(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantID := "tenant-attempt-" + uuid.NewString()[:8]
	queueName := "attempt-q"
	setupTenantAndQueue(t, s, tenantID, queueName)

	jobID := "job-attempt-" + uuid.NewString()[:8]
	job := &domain.Job{
		ID:         jobID,
		TenantID:   tenantID,
		QueueName:  queueName,
		Status:     domain.StatusQueued,
		MaxRetries: 3,
		Payload:    []byte(`{"attempt_test":true}`),
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	// 1. Initial State: attempt must be 0
	j0, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil || j0.Attempt != 0 {
		t.Fatalf("initial attempt must be 0, got %d", j0.Attempt)
	}

	// 2. First Claim: attempt must increment to 1
	claimed1, err := s.ClaimJobs(ctx, tenantID, "w1", []string{queueName}, 1, 30*time.Second)
	if err != nil || len(claimed1) != 1 {
		t.Fatalf("first claim failed: %v", err)
	}
	if claimed1[0].Attempt != 1 {
		t.Fatalf("attempt after first claim must be 1, got %d", claimed1[0].Attempt)
	}

	// Fail with retryable=true and 0 backoff
	err = s.FailJob(ctx, tenantID, jobID, claimed1[0].FencingGeneration, *claimed1[0].LeaseToken, "transient-err-1", true, 0)
	if err != nil {
		t.Fatalf("fail 1 failed: %v", err)
	}

	// State after fail: RETRYING, attempt remains 1
	jRetrying, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil || jRetrying.Status != domain.StatusRetrying || jRetrying.Attempt != 1 {
		t.Fatalf("expected status RETRYING with attempt 1, got status=%s attempt=%d", jRetrying.Status, jRetrying.Attempt)
	}

	// Manually transition to QUEUED (simulate scheduler dispatch for ready retry)
	// Or claim: In ClaimJobs, candidate jobs are status = 'QUEUED'. Let's set it to QUEUED to test second claim:
	// In ForgeFlow, RETRYING jobs whose run_at <= NOW() are picked up or reset to QUEUED.
	// Let's reset to QUEUED via direct update:
	_ = s.RequeueExpiredJob(ctx, tenantID, jobID)

	// 3. Second Claim: attempt must increment to 2
	claimed2, err := s.ClaimJobs(ctx, tenantID, "w2", []string{queueName}, 1, 30*time.Second)
	if err != nil || len(claimed2) != 1 {
		t.Fatalf("second claim failed: %v", err)
	}
	if claimed2[0].Attempt != 2 {
		t.Fatalf("attempt after second claim must be 2, got %d", claimed2[0].Attempt)
	}

	// Fail again retryable
	err = s.FailJob(ctx, tenantID, jobID, claimed2[0].FencingGeneration, *claimed2[0].LeaseToken, "transient-err-2", true, 0)
	if err != nil {
		t.Fatalf("fail 2 failed: %v", err)
	}
	_ = s.RequeueExpiredJob(ctx, tenantID, jobID)

	// 4. Third Claim: attempt must increment to 3 (max_retries reached)
	claimed3, err := s.ClaimJobs(ctx, tenantID, "w3", []string{queueName}, 1, 30*time.Second)
	if err != nil || len(claimed3) != 1 {
		t.Fatalf("third claim failed: %v", err)
	}
	if claimed3[0].Attempt != 3 {
		t.Fatalf("attempt after third claim must be 3, got %d", claimed3[0].Attempt)
	}

	// Fail with attempt == max_retries (3 == 3): MUST become terminal FAILED
	err = s.FailJob(ctx, tenantID, jobID, claimed3[0].FencingGeneration, *claimed3[0].LeaseToken, "fatal-err-final", true, 0)
	if err != nil {
		t.Fatalf("final fail failed: %v", err)
	}

	finalJob, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil {
		t.Fatalf("get final job failed: %v", err)
	}
	if finalJob.Status != domain.StatusFailed {
		t.Fatalf("expected status FAILED after retries exhausted, got: %s", finalJob.Status)
	}
	if finalJob.Attempt != 3 {
		t.Fatalf("attempt must remain 3 on terminal failure, got %d", finalJob.Attempt)
	}
}

// TestPostgres_ReaperStress_ConcurrentAndRaces verifies that multiple concurrent reapers
// safely contend over expired jobs using FOR UPDATE SKIP LOCKED without double-reaping or resurrection.
func TestPostgres_ReaperStress_ConcurrentAndRaces(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantID := "tenant-reaper-" + uuid.NewString()[:8]
	queueName := "reaper-q"
	setupTenantAndQueue(t, s, tenantID, queueName)

	numJobs := 20

	// Enqueue jobs with already-expired leases in RUNNING status
	for i := 0; i < numJobs; i++ {
		job := &domain.Job{
			ID:                fmt.Sprintf("job-reap-%s-%03d", tenantID, i),
			TenantID:          tenantID,
			QueueName:         queueName,
			Status:            domain.StatusQueued,
			MaxRetries:        3,
			Attempt:           1,
			FencingGeneration: 1,
			Payload:           []byte(`{}`),
		}
		if err := s.CreateJob(ctx, job); err != nil {
			t.Fatalf("create job failed: %v", err)
		}
		// Transition to RUNNING with expired lease
		if err := s.SetJobRunningWithExpiredLeaseForTest(ctx, tenantID, job.ID); err != nil {
			t.Fatalf("failed to set job running with expired lease: %v", err)
		}
	}

	// 5 concurrent reapers sweeping simultaneously
	var wg sync.WaitGroup
	numReapers := 5
	wg.Add(numReapers)

	var totalReaped atomic.Int64
	startBarrier := make(chan struct{})

	for r := 0; r < numReapers; r++ {
		go func() {
			defer wg.Done()
			<-startBarrier
			count, err := s.ReapExpiredJobs(ctx, tenantID, 100)
			if err != nil {
				t.Errorf("reap error: %v", err)
				return
			}
			totalReaped.Add(int64(count))
		}()
	}

	close(startBarrier)
	wg.Wait()

	// Total reaped across all reapers must equal exactly numJobs (20)
	// FOR UPDATE SKIP LOCKED ensures no two reapers recover the same job
	if int(totalReaped.Load()) != numJobs {
		t.Fatalf("expected exactly %d reaped jobs across concurrent reapers, got %d", numJobs, totalReaped.Load())
	}

	// Verify all 20 jobs are now QUEUED with incremented fencing_generation (1 -> 2)
	for i := 0; i < numJobs; i++ {
		j, err := s.GetJob(ctx, tenantID, fmt.Sprintf("job-reap-%s-%03d", tenantID, i))
		if err != nil {
			t.Fatalf("get reaped job %d failed: %v", i, err)
		}
		if j.Status != domain.StatusQueued {
			t.Fatalf("job %d expected status QUEUED, got %s", i, j.Status)
		}
		if j.FencingGeneration != 2 {
			t.Fatalf("job %d expected fencing_generation 2, got %d", i, j.FencingGeneration)
		}
		if j.LeaseToken != nil {
			t.Fatalf("job %d expected nil lease_token, got %v", i, *j.LeaseToken)
		}
	}
}
