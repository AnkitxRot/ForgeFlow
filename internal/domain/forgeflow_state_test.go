package domain_test

import (
	"errors"
	"testing"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
)

func TestJobStatus_IsTerminal(t *testing.T) {
	terminalStatuses := []domain.JobStatus{
		domain.StatusCompleted,
		domain.StatusFailed,
		domain.StatusCancelled,
		domain.StatusTimedOut,
	}

	for _, s := range terminalStatuses {
		if !s.IsTerminal() {
			t.Errorf("expected status %s to be terminal", s)
		}
	}

	nonTerminalStatuses := []domain.JobStatus{
		domain.StatusPending,
		domain.StatusScheduled,
		domain.StatusQueued,
		domain.StatusRunning,
		domain.StatusRetrying,
	}

	for _, s := range nonTerminalStatuses {
		if s.IsTerminal() {
			t.Errorf("expected status %s to NOT be terminal", s)
		}
	}
}

func TestValidateJobTransition_ValidTransitions(t *testing.T) {
	validCases := []struct {
		from domain.JobStatus
		to   domain.JobStatus
	}{
		{domain.StatusPending, domain.StatusQueued},
		{domain.StatusPending, domain.StatusCancelled},
		{domain.StatusScheduled, domain.StatusQueued},
		{domain.StatusScheduled, domain.StatusCancelled},
		{domain.StatusQueued, domain.StatusRunning},
		{domain.StatusQueued, domain.StatusCancelled},
		{domain.StatusRunning, domain.StatusRunning},
		{domain.StatusRunning, domain.StatusCompleted},
		{domain.StatusRunning, domain.StatusRetrying},
		{domain.StatusRunning, domain.StatusFailed},
		{domain.StatusRunning, domain.StatusCancelled},
		{domain.StatusRunning, domain.StatusTimedOut},
		{domain.StatusRetrying, domain.StatusQueued},
		{domain.StatusRetrying, domain.StatusCancelled},
	}

	for _, tc := range validCases {
		if err := domain.ValidateJobTransition(tc.from, tc.to); err != nil {
			t.Errorf("expected valid transition from %s to %s, got error: %v", tc.from, tc.to, err)
		}
	}
}

func TestValidateJobTransition_TerminalImmutability(t *testing.T) {
	terminalStates := []domain.JobStatus{
		domain.StatusCompleted,
		domain.StatusFailed,
		domain.StatusCancelled,
		domain.StatusTimedOut,
	}

	allStates := []domain.JobStatus{
		domain.StatusPending,
		domain.StatusScheduled,
		domain.StatusQueued,
		domain.StatusRunning,
		domain.StatusRetrying,
		domain.StatusCompleted,
		domain.StatusFailed,
		domain.StatusCancelled,
		domain.StatusTimedOut,
	}

	for _, term := range terminalStates {
		for _, target := range allStates {
			err := domain.ValidateJobTransition(term, target)
			if err == nil {
				t.Fatalf("critical invariant violated: allowed transition out of terminal state %s -> %s", term, target)
			}
			if !errors.Is(err, domain.ErrTerminalState) {
				t.Errorf("expected ErrTerminalState when moving %s -> %s, got: %v", term, target, err)
			}
		}
	}
}

func TestValidateJobTransition_InvalidNonTerminalTransitions(t *testing.T) {
	invalidCases := []struct {
		from domain.JobStatus
		to   domain.JobStatus
	}{
		{domain.StatusPending, domain.StatusRunning},   // Must be queued before running
		{domain.StatusPending, domain.StatusCompleted}, // Cannot skip execution
		{domain.StatusQueued, domain.StatusCompleted},  // Must be claimed and running first
		{domain.StatusQueued, domain.StatusRetrying},   // Cannot retry a job that hasn't run
		{domain.StatusRetrying, domain.StatusRunning},  // Must be queued after retry delay
		{domain.StatusScheduled, domain.StatusRunning}, // Must be queued first
	}

	for _, tc := range invalidCases {
		err := domain.ValidateJobTransition(tc.from, tc.to)
		if err == nil {
			t.Errorf("expected transition %s -> %s to fail, but it succeeded", tc.from, tc.to)
		}
		if !errors.Is(err, domain.ErrInvalidTransition) {
			t.Errorf("expected ErrInvalidTransition for %s -> %s, got: %v", tc.from, tc.to, err)
		}
	}
}
