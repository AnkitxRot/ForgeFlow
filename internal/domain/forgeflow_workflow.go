package domain

import (
	"errors"
	"time"
)

// WorkflowStatus represents the lifecycle state of an entire DAG workflow execution.
type WorkflowStatus string

const (
	WorkflowStatusPending   WorkflowStatus = "PENDING"
	WorkflowStatusRunning   WorkflowStatus = "RUNNING"
	WorkflowStatusCompleted WorkflowStatus = "COMPLETED"
	WorkflowStatusFailed    WorkflowStatus = "FAILED"
	WorkflowStatusCancelled WorkflowStatus = "CANCELLED"
)

// IsTerminal returns true if the workflow has reached an immutable terminal state.
func (s WorkflowStatus) IsTerminal() bool {
	switch s {
	case WorkflowStatusCompleted, WorkflowStatusFailed, WorkflowStatusCancelled:
		return true
	default:
		return false
	}
}

// FailurePolicy defines how step failure impacts the parent workflow.
type FailurePolicy string

const (
	FailurePolicyFailWorkflow FailurePolicy = "FAIL_WORKFLOW"
	FailurePolicyContinue     FailurePolicy = "CONTINUE"
)

// Workflow represents a parent DAG workflow instance.
type Workflow struct {
	ID             string         `json:"id"`
	TenantID       string         `json:"tenant_id"`
	Name           string         `json:"name"`
	Status         WorkflowStatus `json:"status"`
	IdempotencyKey *string        `json:"idempotency_key,omitempty"`
	DefinitionJSON []byte         `json:"definition_json"`
	ContextData    []byte         `json:"context_data"`
	ErrorMessage   *string        `json:"error_message,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	StartedAt      *time.Time     `json:"started_at,omitempty"`
	CompletedAt    *time.Time     `json:"completed_at,omitempty"`
}

// WorkflowStep represents an individual node in the workflow DAG.
type WorkflowStep struct {
	ID            string        `json:"id"`
	WorkflowID    string        `json:"workflow_id"`
	StepName      string        `json:"step_name"`
	Status        JobStatus     `json:"status"`
	Dependencies  []string      `json:"dependencies"` // Slice of step names
	Handler       string        `json:"handler"`
	InputTemplate []byte        `json:"input_template"`
	OutputData    []byte        `json:"output_data,omitempty"`
	ErrorMessage  *string       `json:"error_message,omitempty"`
	FailurePolicy FailurePolicy `json:"failure_policy"`
	CreatedAt     time.Time     `json:"created_at"`
	CompletedAt   *time.Time    `json:"completed_at,omitempty"`
}

func (w *Workflow) ValidateCreation() error {
	if w.TenantID == "" {
		return ErrEmptyTenantID
	}
	if w.ID == "" {
		return errors.New("workflow id must not be empty")
	}
	if w.Name == "" {
		return errors.New("workflow name must not be empty")
	}
	if w.Status == "" {
		w.Status = WorkflowStatusPending
	}
	if w.DefinitionJSON == nil {
		w.DefinitionJSON = []byte("{}")
	}
	if w.ContextData == nil {
		w.ContextData = []byte("{}")
	}
	return nil
}
