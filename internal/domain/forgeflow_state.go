package domain

import (
	"errors"
	"fmt"
)

// JobStatus represents the lifecycle state of an individual job.
type JobStatus string

const (
	StatusPending   JobStatus = "PENDING"
	StatusScheduled JobStatus = "SCHEDULED"
	StatusQueued    JobStatus = "QUEUED"
	StatusRunning   JobStatus = "RUNNING"
	StatusRetrying  JobStatus = "RETRYING"
	StatusCompleted JobStatus = "COMPLETED"
	StatusFailed    JobStatus = "FAILED"
	StatusCancelled JobStatus = "CANCELLED"
	StatusTimedOut  JobStatus = "TIMED_OUT"
)

// IsTerminal returns true if the status represents an unalterable terminal state.
func (s JobStatus) IsTerminal() bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusCancelled, StatusTimedOut:
		return true
	default:
		return false
	}
}

// IsActive returns true if the status represents an actively progressing or claimable state.
func (s JobStatus) IsActive() bool {
	switch s {
	case StatusPending, StatusScheduled, StatusQueued, StatusRunning, StatusRetrying:
		return true
	default:
		return false
	}
}

var (
	// ErrInvalidTransition is returned when attempting an illegal state transition.
	ErrInvalidTransition = errors.New("invalid job state transition")
	// ErrTerminalState is returned when attempting to mutate a job that is already in a terminal state.
	ErrTerminalState = errors.New("job is in an immutable terminal state")
)

// validTransitions defines the exhaustive directed graph of allowed job state transitions.
var validTransitions = map[JobStatus]map[JobStatus]bool{
	StatusPending: {
		StatusQueued:    true,
		StatusCancelled: true,
	},
	StatusScheduled: {
		StatusQueued:    true,
		StatusCancelled: true,
	},
	StatusQueued: {
		StatusRunning:   true,
		StatusCancelled: true,
	},
	StatusRunning: {
		StatusRunning:   true, // Heartbeat / lease renewal
		StatusRetrying:  true, // Retryable failure / lease expiration with retries left
		StatusCompleted: true, // Successful ack
		StatusFailed:    true, // Fatal error or retries exhausted
		StatusCancelled: true, // Explicit user/admin cancellation
		StatusTimedOut:  true, // Hard deadline exceeded
	},
	StatusRetrying: {
		StatusQueued:    true, // Delayed backoff timer elapsed
		StatusCancelled: true, // Cancelled while waiting for backoff
	},
	// Terminal states have zero allowed outbound transitions
	StatusCompleted: {},
	StatusFailed:    {},
	StatusCancelled: {},
	StatusTimedOut:  {},
}

// ValidateJobTransition verifies whether transitioning from `from` to `to` is legally permitted.
func ValidateJobTransition(from, to JobStatus) error {
	if from.IsTerminal() {
		return fmt.Errorf("%w: cannot transition out of terminal state %s to %s", ErrTerminalState, from, to)
	}

	allowed, ok := validTransitions[from]
	if !ok || !allowed[to] {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}

	return nil
}
