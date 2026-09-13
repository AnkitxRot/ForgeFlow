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
