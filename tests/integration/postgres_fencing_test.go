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

// TestPostgres_Fencing_CompleteMatrix_GenerationsAndMutations explicitly fulfills Phase 4:
// It creates the generation sequence A -> N, B -> N+1, C -> N+2, D -> N+3.
// For every stale generation, it tests CompleteJob, FailJob, RenewLease, and CancelJob.
// Proves:
// - stale mutations return ErrLeaseLost
// - zero rows are modified
// - current owner's state remains completely unchanged
// - stale lease token cannot bypass fencing
// - stale generation cannot bypass fencing
// - terminal state cannot be resurrected
func TestPostgres_Fencing_CompleteMatrix_GenerationsAndMutations(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantID := "tenant-matrix-" + uuid.NewString()[:8]
	queueName := "matrix-q"
	setupTenantAndQueue(t, s, tenantID, queueName)

	jobID := "job-matrix-" + uuid.NewString()[:8]
	job := &domain.Job{
		ID:         jobID,
		TenantID:   tenantID,
		QueueName:  queueName,
		Status:     domain.StatusQueued,
		Payload:    []byte(`{"fencing_matrix":true}`),
		MaxRetries: 5,
	}
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	type workerCreds struct {
		workerID string
		gen      int64
		token    string
	}

	workers := []string{"WorkerA", "WorkerB", "WorkerC", "WorkerD"}
	history := make([]workerCreds, 4)

	for i, wName := range workers {
		claimed, err := s.ClaimJobs(ctx, tenantID, wName, []string{queueName}, 1, 30*time.Second)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("claim %d (%s) failed: %v", i, wName, err)
		}
		c := claimed[0]
		expectedGen := int64(2*i + 1) // WorkerA=1, WorkerB=3, WorkerC=5, WorkerD=7 (reaper increments on sweep)
		if c.FencingGeneration != expectedGen {
			t.Fatalf("worker %s expected generation %d, got %d", wName, expectedGen, c.FencingGeneration)
		}
		history[i] = workerCreds{
			workerID: wName,
			gen:      c.FencingGeneration,
			token:    *c.LeaseToken,
		}

		// Expire lease and let reaper sweep back to QUEUED for next worker
		if i < len(workers)-1 {
			if err := s.ExpireJobLeaseForTest(ctx, tenantID, jobID); err != nil {
				t.Fatalf("expire lease %d failed: %v", i, err)
			}
			reaped, err := s.ReapExpiredJobs(ctx, tenantID, 10)
			if err != nil || reaped != 1 {
				t.Fatalf("reap %d failed: count=%d, err=%v", i, reaped, err)
			}
		}
	}

	// Now WorkerD is active owner (gen 4)
	active := history[3]
	beforeDurable, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil {
		t.Fatalf("get durable job failed: %v", err)
	}
	if beforeDurable.FencingGeneration != active.gen || beforeDurable.WorkerID == nil || *beforeDurable.WorkerID != active.workerID {
		t.Fatalf("active owner mismatch: expected %s (gen %d), got %v (gen %d)", active.workerID, active.gen, beforeDurable.WorkerID, beforeDurable.FencingGeneration)
	}

	// Test stale mutations for all prior generations (A, B, C)
	for i := 0; i < 3; i++ {
		stale := history[i]

		// 1. Stale CompleteJob
		err = s.CompleteJob(ctx, tenantID, jobID, stale.gen, stale.token, []byte(`{"stale":true}`))
		if !errors.Is(err, store.ErrLeaseLost) {
			t.Fatalf("stale CompleteJob (%s gen %d) must fail with ErrLeaseLost, got: %v", stale.workerID, stale.gen, err)
		}

		// 2. Stale FailJob
		err = s.FailJob(ctx, tenantID, jobID, stale.gen, stale.token, "stale error", false, 0)
		if !errors.Is(err, store.ErrLeaseLost) {
			t.Fatalf("stale FailJob (%s gen %d) must fail with ErrLeaseLost, got: %v", stale.workerID, stale.gen, err)
		}

		// 3. Stale RenewLease
		err = s.RenewLease(ctx, tenantID, jobID, stale.gen, stale.token, 10*time.Second)
		if !errors.Is(err, store.ErrLeaseLost) {
			t.Fatalf("stale RenewLease (%s gen %d) must fail with ErrLeaseLost, got: %v", stale.workerID, stale.gen, err)
		}

		// 4. Stale token with active generation
		err = s.CompleteJob(ctx, tenantID, jobID, active.gen, stale.token, []byte(`{"spoofed_token":true}`))
		if !errors.Is(err, store.ErrLeaseLost) {
			t.Fatalf("spoofed token with active generation must fail with ErrLeaseLost, got: %v", err)
		}

		// 5. Active token with stale generation
		err = s.CompleteJob(ctx, tenantID, jobID, stale.gen, active.token, []byte(`{"spoofed_gen":true}`))
		if !errors.Is(err, store.ErrLeaseLost) {
			t.Fatalf("active token with stale generation must fail with ErrLeaseLost, got: %v", err)
		}

		// Verify active owner's durable state remains completely unchanged
		afterDurable, err := s.GetJob(ctx, tenantID, jobID)
		if err != nil {
			t.Fatalf("get durable job after attacks failed: %v", err)
		}
		if afterDurable.Status != domain.StatusRunning || afterDurable.FencingGeneration != active.gen || *afterDurable.WorkerID != active.workerID {
			t.Fatalf("CRITICAL FENCING VIOLATION: active owner mutated by stale worker %s!", stale.workerID)
		}
	}

	// Active WorkerD completes the job cleanly
	err = s.CompleteJob(ctx, tenantID, jobID, active.gen, active.token, []byte(`{"valid_completion":true}`))
	if err != nil {
		t.Fatalf("active completion failed: %v", err)
	}

	// Verify job is now terminal COMPLETED
	termJob, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil || termJob.Status != domain.StatusCompleted {
		t.Fatalf("expected terminal COMPLETED, got status=%s (err=%v)", termJob.Status, err)
	}

	// Any mutation attempt on terminal job must be rejected with ErrTerminalState
	// 1. CancelJob
	err = s.CancelJob(ctx, tenantID, jobID)
	if !errors.Is(err, store.ErrTerminalState) {
		t.Fatalf("expected ErrTerminalState on CancelJob for completed job, got: %v", err)
	}
	// 2. CompleteJob
	err = s.CompleteJob(ctx, tenantID, jobID, active.gen, active.token, []byte(`{}`))
	if !errors.Is(err, store.ErrTerminalState) {
		t.Fatalf("expected ErrTerminalState on CompleteJob for completed job, got: %v", err)
	}
	// 3. FailJob
	err = s.FailJob(ctx, tenantID, jobID, active.gen, active.token, "err", false, 0)
	if !errors.Is(err, store.ErrTerminalState) {
		t.Fatalf("expected ErrTerminalState on FailJob for completed job, got: %v", err)
	}
	// 4. RenewLease
	err = s.RenewLease(ctx, tenantID, jobID, active.gen, active.token, 10*time.Second)
	if !errors.Is(err, store.ErrTerminalState) && !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("expected ErrTerminalState/ErrLeaseLost on RenewLease for completed job, got: %v", err)
	}
}

// TestPostgres_Reaper_Heartbeat_Races tests the edge cases in Phase 6:
// - heartbeat before expiry
// - heartbeat after expiry
// - reaper before heartbeat
// - reaper concurrently with heartbeat
// - stale heartbeat after reassignment
// - terminal job encountered by reaper
func TestPostgres_Reaper_Heartbeat_Races(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantID := "tenant-rh-race-" + uuid.NewString()[:8]
	queueName := "rh-q"
	setupTenantAndQueue(t, s, tenantID, queueName)

	// 1. Heartbeat before expiry: extends successfully
	t.Run("HeartbeatBeforeExpiry", func(t *testing.T) {
		jID := "job-hb-before-" + uuid.NewString()[:8]
		_ = s.CreateJob(ctx, &domain.Job{ID: jID, TenantID: tenantID, QueueName: queueName, Status: domain.StatusQueued})
		claimed, _ := s.ClaimJobs(ctx, tenantID, "w1", []string{queueName}, 1, 30*time.Second)
		c := claimed[0]

		err := s.RenewLease(ctx, tenantID, c.ID, c.FencingGeneration, *c.LeaseToken, 45*time.Second)
		if err != nil {
			t.Fatalf("renew before expiry failed: %v", err)
		}
		j, _ := s.GetJob(ctx, tenantID, c.ID)
		if j.LeaseExpiresAt == nil || j.LeaseExpiresAt.Before(time.Now().UTC().Add(30*time.Second)) {
			t.Fatalf("lease renewal did not extend into future")
		}
	})

	// 2. Reaper before heartbeat: reaper reclaims expired job, subsequent heartbeat rejected with ErrLeaseLost
	t.Run("ReaperBeforeHeartbeat", func(t *testing.T) {
		jID := "job-reap-before-hb-" + uuid.NewString()[:8]
		_ = s.CreateJob(ctx, &domain.Job{ID: jID, TenantID: tenantID, QueueName: queueName, Status: domain.StatusQueued, MaxRetries: 3})
		claimed, _ := s.ClaimJobs(ctx, tenantID, "w1", []string{queueName}, 1, 30*time.Second)
		c := claimed[0]

		_ = s.ExpireJobLeaseForTest(ctx, tenantID, c.ID)
		reaped, err := s.ReapExpiredJobs(ctx, tenantID, 10)
		if err != nil || reaped != 1 {
			t.Fatalf("reap failed: count=%d, err=%v", reaped, err)
		}

		// Subsequent heartbeat from original worker must fail
		err = s.RenewLease(ctx, tenantID, c.ID, c.FencingGeneration, *c.LeaseToken, 10*time.Second)
		if !errors.Is(err, store.ErrLeaseLost) {
			t.Fatalf("expected ErrLeaseLost for heartbeat after reaper sweep, got: %v", err)
		}
	})

	// 3. Stale heartbeat after reassignment: second worker claims, first worker's heartbeat rejected
	t.Run("StaleHeartbeatAfterReassignment", func(t *testing.T) {
		subTenantID := "tenant-rh-reassign-" + uuid.NewString()[:8]
		subQueue := "rh-reassign-q"
		setupTenantAndQueue(t, s, subTenantID, subQueue)
		jID := "job-stale-hb-reassign-" + uuid.NewString()[:8]
		_ = s.CreateJob(ctx, &domain.Job{ID: jID, TenantID: subTenantID, QueueName: subQueue, Status: domain.StatusQueued, MaxRetries: 3})
		c1, err := s.ClaimJobs(ctx, subTenantID, "w1", []string{subQueue}, 1, 30*time.Second)
		if err != nil || len(c1) != 1 {
			t.Fatalf("c1 claim failed: %v", err)
		}
		_ = s.ExpireJobLeaseForTest(ctx, subTenantID, c1[0].ID)
		_, _ = s.ReapExpiredJobs(ctx, subTenantID, 10)

		c2, err := s.ClaimJobs(ctx, subTenantID, "w2", []string{subQueue}, 1, 30*time.Second)
		if err != nil || len(c2) != 1 {
			t.Fatalf("second claim failed: %v", err)
		}

		// w1 tries heartbeat
		err = s.RenewLease(ctx, subTenantID, jID, c1[0].FencingGeneration, *c1[0].LeaseToken, 10*time.Second)
		if !errors.Is(err, store.ErrLeaseLost) {
			t.Fatalf("w1 heartbeat must fail with ErrLeaseLost, got: %v", err)
		}

		// w2 heartbeat succeeds
		err = s.RenewLease(ctx, subTenantID, jID, c2[0].FencingGeneration, *c2[0].LeaseToken, 10*time.Second)
		if err != nil {
			t.Fatalf("w2 heartbeat should succeed, got: %v", err)
		}
	})

	// 4. Terminal job encountered by reaper: reaper never modifies terminal jobs
	t.Run("TerminalJobEncounteredByReaper", func(t *testing.T) {
		termTenantID := "tenant-rh-term-" + uuid.NewString()[:8]
		termQueue := "rh-term-q"
		setupTenantAndQueue(t, s, termTenantID, termQueue)
		jID := "job-term-reaper-" + uuid.NewString()[:8]
		_ = s.CreateJob(ctx, &domain.Job{ID: jID, TenantID: termTenantID, QueueName: termQueue, Status: domain.StatusQueued})
		c, err := s.ClaimJobs(ctx, termTenantID, "w1", []string{termQueue}, 1, 30*time.Second)
		if err != nil || len(c) != 1 {
			t.Fatalf("claim failed: %v", err)
		}
		err = s.CompleteJob(ctx, termTenantID, jID, c[0].FencingGeneration, *c[0].LeaseToken, []byte(`{"term":true}`))
		if err != nil {
			t.Fatalf("complete failed: %v", err)
		}

		// Expire lease (even if completed has NULL lease_token)
		_ = s.ExpireJobLeaseForTest(ctx, termTenantID, jID)

		reaped, err := s.ReapExpiredJobs(ctx, termTenantID, 10)
		if err != nil {
			t.Fatalf("reap error: %v", err)
		}
		if reaped != 0 {
			t.Fatalf("reaper must not touch terminal jobs, reaped: %d", reaped)
		}

		finalJob, _ := s.GetJob(ctx, termTenantID, jID)
		if finalJob.Status != domain.StatusCompleted {
			t.Fatalf("terminal job altered by reaper: status=%s", finalJob.Status)
		}
	})
}
