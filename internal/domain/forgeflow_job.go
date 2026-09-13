package domain

import (
	"errors"
	"time"
)

var (
	ErrEmptyTenantID = errors.New("tenant_id must not be empty")
	ErrEmptyJobID    = errors.New("job id must not be empty")
	ErrEmptyQueue    = errors.New("queue name must not be empty")
)

// Job represents an atomic unit of work persisted and executed within ForgeFlow.
type Job struct {
	ID                  string     `json:"id"`
	TenantID            string     `json:"tenant_id"`
	QueueName           string     `json:"queue_name"`
	WorkflowID          *string    `json:"workflow_id,omitempty"`
	WorkflowStepID      *string    `json:"workflow_step_id,omitempty"`
	Status              JobStatus  `json:"status"`
	Priority            int        `json:"priority"`
	Payload             []byte     `json:"payload"`
	Result              []byte     `json:"result,omitempty"`
	ErrorMessage        *string    `json:"error_message,omitempty"`
	Attempt             int        `json:"attempt"`
	FencingGeneration   int64      `json:"fencing_generation"`
	MaxRetries          int        `json:"max_retries"`
	RetryBackoffSeconds int        `json:"retry_backoff_seconds"`
	TimeoutSeconds      int        `json:"timeout_seconds"`
	RunAt               time.Time  `json:"run_at"`
	LeaseToken          *string    `json:"lease_token,omitempty"`
	LeaseExpiresAt      *time.Time `json:"lease_expires_at,omitempty"`
	WorkerID            *string    `json:"worker_id,omitempty"`
	IdempotencyKey      *string    `json:"idempotency_key,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
	CompletedAt         *time.Time `json:"completed_at,omitempty"`
}

// ValidateCreation checks baseline integrity invariants for submitting a new job.
func (j *Job) ValidateCreation() error {
	if j.TenantID == "" {
		return ErrEmptyTenantID
	}
	if j.ID == "" {
		return ErrEmptyJobID
	}
	if j.QueueName == "" {
		return ErrEmptyQueue
	}
	if j.Status == "" {
		j.Status = StatusQueued
	}
	if j.Status != StatusQueued && j.Status != StatusScheduled && j.Status != StatusPending {
		return errors.New("initial job status must be QUEUED, SCHEDULED, or PENDING")
	}
	if j.MaxRetries < 0 {
		j.MaxRetries = 3
	}
	if j.RetryBackoffSeconds <= 0 {
		j.RetryBackoffSeconds = 5
	}
	if j.TimeoutSeconds <= 0 {
		j.TimeoutSeconds = 300
	}
	if j.RunAt.IsZero() {
		j.RunAt = time.Now().UTC()
	}
	if j.Payload == nil {
		j.Payload = []byte("{}")
	}
	return nil
}

// JobExecution represents an immutable historical record of a single execution attempt.
type JobExecution struct {
	ID                string     `json:"id"`
	JobID             string     `json:"job_id"`
	Attempt           int        `json:"attempt"`
	FencingGeneration int64      `json:"fencing_generation"`
	WorkerID          string     `json:"worker_id"`
	Status            JobStatus  `json:"status"`
	ErrorMessage      *string    `json:"error_message,omitempty"`
	StartedAt         time.Time  `json:"started_at"`
	FinishedAt        *time.Time `json:"finished_at,omitempty"`
	DurationMs        *int64     `json:"duration_ms,omitempty"`
}
