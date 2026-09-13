package worker

import "fmt"

// State represents the lifecycle state of a ForgeFlow worker instance.
type State string

const (
	StateStarting State = "STARTING"
	StateRunning  State = "RUNNING"
	StateDraining State = "DRAINING"
	StateStopped  State = "STOPPED"
)

// IsActive returns true if the worker is in a state capable of processing or finishing jobs.
func (s State) IsActive() bool {
	return s == StateRunning || s == StateDraining
}

// CanClaim returns true if the worker is allowed to poll and claim new work.
func (s State) CanClaim() bool {
	return s == StateRunning
}

// ValidateTransition validates that a worker lifecycle transition is legal.
func ValidateTransition(from, to State) error {
	switch from {
	case StateStarting:
		if to == StateRunning || to == StateStopped {
			return nil
		}
	case StateRunning:
		if to == StateDraining || to == StateStopped {
			return nil
		}
	case StateDraining:
		if to == StateStopped {
			return nil
		}
	case StateStopped:
		return fmt.Errorf("cannot transition from terminal worker state %s", from)
	}
	return fmt.Errorf("invalid worker state transition from %s to %s", from, to)
}
