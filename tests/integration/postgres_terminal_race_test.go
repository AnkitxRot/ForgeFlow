package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
)

// TestPostgres_TerminalRace_CompletionVsCancellation tests concurrent completion vs cancellation.
// Exactly one operation must succeed. The other must return ErrTerminalState.
func TestPostgres_TerminalRace_CompletionVsCancellation(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantID := "tenant-race-comp-canc-" + uuid.NewString()[:8]
	queueName := "race-q"
	setupTenantAndQueue(t, s, tenantID, queueName)

	trials := 20
	for i := 0; i < trials; i++ {
		jobID := fmt.Sprintf("job-comp-canc-%s-%03d", tenantID, i)
		_ = s.CreateJob(ctx, &domain.Job{
			ID:        jobID,
			TenantID:  tenantID,
			QueueName: queueName,
			Status:    domain.StatusQueued,
			Payload:   []byte(`{}`),
		})

		claimed, err := s.ClaimJobs(ctx, tenantID, "worker-race", []string{queueName}, 1, 30*time.Second)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("trial %d claim failed: %v", i, err)
		}
		cJob := claimed[0]

		var compErr, cancErr error
		var wg sync.WaitGroup
		wg.Add(2)

		barrier := make(chan struct{})

		go func() {
			defer wg.Done()
			<-barrier
			compErr = s.CompleteJob(ctx, tenantID, cJob.ID, cJob.FencingGeneration, *cJob.LeaseToken, []byte(`{"out":true}`))
		}()

		go func() {
			defer wg.Done()
			<-barrier
			cancErr = s.CancelJob(ctx, tenantID, cJob.ID)
		}()

		close(barrier)
		wg.Wait()

		// Final state must be terminal
		finalJob, err := s.GetJob(ctx, tenantID, cJob.ID)
		if err != nil {
			t.Fatalf("trial %d get job failed: %v", i, err)
		}

		if finalJob.Status == domain.StatusCompleted {
			if compErr != nil {
				t.Fatalf("trial %d: job is COMPLETED but CompleteJob returned: %v", i, compErr)
			}
			if !errors.Is(cancErr, store.ErrTerminalState) {
				t.Fatalf("trial %d: expected CancelJob to return ErrTerminalState, got: %v", i, cancErr)
			}
		} else if finalJob.Status == domain.StatusCancelled {
			if cancErr != nil {
				t.Fatalf("trial %d: job is CANCELLED but CancelJob returned: %v", i, cancErr)
			}
			if !errors.Is(compErr, store.ErrTerminalState) && !errors.Is(compErr, store.ErrLeaseLost) {
				t.Fatalf("trial %d: expected CompleteJob to return ErrTerminalState or ErrLeaseLost, got: %v", i, compErr)
			}
		} else {
			t.Fatalf("trial %d: unexpected non-terminal state %s", i, finalJob.Status)
		}
	}
}

// TestPostgres_TerminalRace_FailureVsCancellation tests concurrent failure vs cancellation.
func TestPostgres_TerminalRace_FailureVsCancellation(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantID := "tenant-race-fail-canc-" + uuid.NewString()[:8]
	queueName := "race-q"
	setupTenantAndQueue(t, s, tenantID, queueName)

	trials := 15
	for i := 0; i < trials; i++ {
		jobID := fmt.Sprintf("job-fail-canc-%s-%03d", tenantID, i)
		_ = s.CreateJob(ctx, &domain.Job{
			ID:         jobID,
			TenantID:   tenantID,
			QueueName:  queueName,
			Status:     domain.StatusQueued,
			MaxRetries: 1, // Will exhaust on 1st failure -> FAILED
			Payload:    []byte(`{}`),
		})

		claimed, err := s.ClaimJobs(ctx, tenantID, "worker-race", []string{queueName}, 1, 30*time.Second)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("trial %d claim failed: %v", i, err)
		}
		cJob := claimed[0]

		var failErr, cancErr error
		var wg sync.WaitGroup
		wg.Add(2)

		barrier := make(chan struct{})

		go func() {
			defer wg.Done()
			<-barrier
			failErr = s.FailJob(ctx, tenantID, cJob.ID, cJob.FencingGeneration, *cJob.LeaseToken, "fail race", true, 0)
		}()

		go func() {
			defer wg.Done()
			<-barrier
			cancErr = s.CancelJob(ctx, tenantID, cJob.ID)
		}()

		close(barrier)
		wg.Wait()

		finalJob, err := s.GetJob(ctx, tenantID, cJob.ID)
		if err != nil {
			t.Fatalf("trial %d get job failed: %v", i, err)
		}

		if !finalJob.Status.IsTerminal() {
			t.Fatalf("trial %d: expected terminal status, got %s", i, finalJob.Status)
		}
		if failErr != nil && cancErr != nil {
			t.Fatalf("trial %d: both failure and cancellation returned error: failErr=%v, cancErr=%v", i, failErr, cancErr)
		}
	}
}

// TestPostgres_TerminalRace_ReaperVsCompletion tests concurrent reaper sweep vs worker completion.
func TestPostgres_TerminalRace_ReaperVsCompletion(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantID := "tenant-race-reap-comp-" + uuid.NewString()[:8]
	queueName := "race-q"
	setupTenantAndQueue(t, s, tenantID, queueName)

	trials := 15
	for i := 0; i < trials; i++ {
		jobID := fmt.Sprintf("job-reap-comp-%s-%03d", tenantID, i)
		_ = s.CreateJob(ctx, &domain.Job{
			ID:         jobID,
			TenantID:   tenantID,
			QueueName:  queueName,
			Status:     domain.StatusQueued,
			MaxRetries: 3,
			Payload:    []byte(`{}`),
		})

		claimed, err := s.ClaimJobs(ctx, tenantID, "worker-race", []string{queueName}, 1, 30*time.Second)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("trial %d claim failed: %v", i, err)
		}
		cJob := claimed[0]

		// Set lease to expired
		_ = s.ExpireJobLeaseForTest(ctx, tenantID, cJob.ID)

		var reapCount int
		var compErr error
		var wg sync.WaitGroup
		wg.Add(2)

		barrier := make(chan struct{})

		go func() {
			defer wg.Done()
			<-barrier
			reapCount, _ = s.ReapExpiredJobs(ctx, tenantID, 10)
		}()

		go func() {
			defer wg.Done()
			<-barrier
			compErr = s.CompleteJob(ctx, tenantID, cJob.ID, cJob.FencingGeneration, *cJob.LeaseToken, []byte(`{"result":1}`))
		}()

		close(barrier)
		wg.Wait()

		finalJob, err := s.GetJob(ctx, tenantID, cJob.ID)
		if err != nil {
			t.Fatalf("get job failed: %v", err)
		}

		// Either reaped (status = QUEUED) or completed (status = COMPLETED)
		// Never broken or double-state
		if finalJob.Status == domain.StatusCompleted {
			if compErr != nil {
				t.Fatalf("expected nil compErr on completed job, got %v", compErr)
			}
		} else if finalJob.Status == domain.StatusQueued {
			if reapCount != 1 {
				t.Fatalf("expected reapCount 1, got %d", reapCount)
			}
			if !errors.Is(compErr, store.ErrLeaseLost) {
				t.Fatalf("expected ErrLeaseLost on stale completion, got: %v", compErr)
			}
		} else {
			t.Fatalf("unexpected state after race: %s", finalJob.Status)
		}
	}
}

// TestPostgres_Terminal_AllTerminalStates_Immutable proves Phase 8:
// For all four terminal states (COMPLETED, FAILED, CANCELLED, TIMED_OUT),
// no subsequent mutation (complete, fail, cancel, renew, reap) can resurrect or modify the job.
func TestPostgres_Terminal_AllTerminalStates_Immutable(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantID := "tenant-term-audit-" + uuid.NewString()[:8]
	queueName := "term-audit-q"
	setupTenantAndQueue(t, s, tenantID, queueName)

	terminalStates := []domain.JobStatus{
		domain.StatusCompleted,
		domain.StatusFailed,
		domain.StatusCancelled,
		domain.StatusTimedOut,
	}

	for _, termStatus := range terminalStates {
		t.Run(string(termStatus), func(t *testing.T) {
			jobID := fmt.Sprintf("job-immut-%s-%s", termStatus, uuid.NewString()[:8])
			err := s.CreateJob(ctx, &domain.Job{
				ID:        jobID,
				TenantID:  tenantID,
				QueueName: queueName,
				Status:    domain.StatusQueued,
				Payload:   []byte(`{"term":true}`),
			})
			if err != nil {
				t.Fatalf("create job failed: %v", err)
			}

			claimed, err := s.ClaimJobs(ctx, tenantID, "w-term", []string{queueName}, 1, 30*time.Second)
			if err != nil || len(claimed) != 1 {
				t.Fatalf("claim failed: %v", err)
			}
			cJob := claimed[0]

			// Transition job to target terminal state
			switch termStatus {
			case domain.StatusCompleted:
				err = s.CompleteJob(ctx, tenantID, cJob.ID, cJob.FencingGeneration, *cJob.LeaseToken, []byte(`{"res":true}`))
			case domain.StatusFailed:
				err = s.FailJob(ctx, tenantID, cJob.ID, cJob.FencingGeneration, *cJob.LeaseToken, "terminal error", false, 0)
			case domain.StatusCancelled:
				err = s.CancelJob(ctx, tenantID, cJob.ID)
			case domain.StatusTimedOut:
				_ = s.ExpireJobLeaseForTest(ctx, tenantID, cJob.ID)
				// Set attempt = max_retries to force TIMED_OUT on reap
				_, err = s.ReapExpiredJobs(ctx, tenantID, 10)
			}
			if err != nil {
				t.Fatalf("transition to %s failed: %v", termStatus, err)
			}

			// Verify current state is terminal
			curJob, err := s.GetJob(ctx, tenantID, cJob.ID)
			if err != nil {
				t.Fatalf("get job failed: %v", err)
			}
			if curJob.Status != termStatus {
				t.Fatalf("expected terminal status %s, got %s", termStatus, curJob.Status)
			}

			// 1. Attempt CompleteJob
			err = s.CompleteJob(ctx, tenantID, cJob.ID, cJob.FencingGeneration, "any-token", []byte(`{"hacked":true}`))
			if !errors.Is(err, store.ErrTerminalState) && !errors.Is(err, store.ErrLeaseLost) {
				t.Fatalf("CompleteJob on %s must return ErrTerminalState/ErrLeaseLost, got: %v", termStatus, err)
			}

			// 2. Attempt FailJob
			err = s.FailJob(ctx, tenantID, cJob.ID, cJob.FencingGeneration, "any-token", "fail hack", true, 0)
			if !errors.Is(err, store.ErrTerminalState) && !errors.Is(err, store.ErrLeaseLost) {
				t.Fatalf("FailJob on %s must return ErrTerminalState/ErrLeaseLost, got: %v", termStatus, err)
			}

			// 3. Attempt CancelJob
			err = s.CancelJob(ctx, tenantID, cJob.ID)
			if !errors.Is(err, store.ErrTerminalState) {
				t.Fatalf("CancelJob on %s must return ErrTerminalState, got: %v", termStatus, err)
			}

			// 4. Attempt RenewLease
			err = s.RenewLease(ctx, tenantID, cJob.ID, cJob.FencingGeneration, "any-token", 10*time.Second)
			if !errors.Is(err, store.ErrTerminalState) && !errors.Is(err, store.ErrLeaseLost) {
				t.Fatalf("RenewLease on %s must return ErrTerminalState/ErrLeaseLost, got: %v", termStatus, err)
			}

			// 5. Attempt Reaper Sweep
			_ = s.ExpireJobLeaseForTest(ctx, tenantID, cJob.ID)
			reaped, err := s.ReapExpiredJobs(ctx, tenantID, 10)
			if err != nil {
				t.Fatalf("reap on %s returned error: %v", termStatus, err)
			}
			if reaped != 0 {
				t.Fatalf("reaper resurrected terminal job %s! reaped: %d", termStatus, reaped)
			}

			// Final check: status remains completely unchanged
			finalJob, err := s.GetJob(ctx, tenantID, cJob.ID)
			if err != nil || finalJob.Status != termStatus {
				t.Fatalf("job %s resurrected or altered: now %s", termStatus, finalJob.Status)
			}
		})
	}
}
