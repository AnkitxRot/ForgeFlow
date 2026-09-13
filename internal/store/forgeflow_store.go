package store

import (
	"context"
	"errors"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
)

var (
	// ErrNotFound is returned when an entity does not exist under the requested tenant.
	ErrNotFound = errors.New("entity not found")
	// ErrConflict is returned on unique constraint violation (e.g., idempotency key collision).
	ErrConflict = errors.New("entity conflict or idempotency violation")
	// ErrLeaseLost is returned when a worker mutation is rejected due to a stale fencing generation,
	// expired lease, or mismatched lease token.
	ErrLeaseLost = errors.New("lease expired, revoked, or fencing generation superseded")
	// ErrTerminalState is returned when attempting to mutate an immutable terminal job.
	ErrTerminalState = domain.ErrTerminalState
	// ErrInvalidTransition is returned when an illegal state change is requested.
	ErrInvalidTransition = domain.ErrInvalidTransition
)

// Store defines the persistent storage and transactional mutation interface for ForgeFlow.
// Every method operating on tenant resources strictly requires a tenantID boundary.
type Store interface {
	// Tenant Management
	CreateTenant(ctx context.Context, tenant *domain.Tenant) error
	GetTenant(ctx context.Context, id string) (*domain.Tenant, error)

	// Queue Management
	CreateQueue(ctx context.Context, queue *domain.Queue) error
	GetQueue(ctx context.Context, tenantID, name string) (*domain.Queue, error)

	// Job Lifecycle & Claiming
	CreateJob(ctx context.Context, job *domain.Job) error
	GetJob(ctx context.Context, tenantID, id string) (*domain.Job, error)

	// ClaimJobs atomically claims up to batchSize eligible jobs matching the specified queues.
	// Candidate jobs must be in QUEUED status and run_at <= NOW().
	// Ordering is strictly: priority DESC, run_at ASC, id ASC.
	// Each claimed job advances fencing_generation, increments attempt, generates a unique lease_token,
	// and sets lease_expires_at = NOW() + leaseDuration.
	ClaimJobs(ctx context.Context, tenantID, workerID string, queues []string, batchSize int, leaseDuration time.Duration) ([]*domain.Job, error)

	// Fencing-Protected Worker Mutations
	// CompleteJob acknowledges successful completion. Requires current active fencing_generation and lease_token.
	CompleteJob(ctx context.Context, tenantID, id string, fencingGen int64, leaseToken string, result []byte) error

	// FailJob records task failure. If retryable and attempt < max_retries, transitions to RETRYING with backoff.
	// Otherwise transitions to FAILED. Requires current active fencing_generation and lease_token.
	FailJob(ctx context.Context, tenantID, id string, fencingGen int64, leaseToken string, errMsg string, retryable bool, backoff time.Duration) error

	// RenewLease extends the active lease duration. Requires current active fencing_generation and lease_token.
	RenewLease(ctx context.Context, tenantID, id string, fencingGen int64, leaseToken string, duration time.Duration) error

	// CancelJob transitions an active, queued, or pending job to CANCELLED.
	// Returns ErrTerminalState if the job is already in COMPLETED, FAILED, CANCELLED, or TIMED_OUT.
	CancelJob(ctx context.Context, tenantID, id string) error

	// Execution History
	CreateExecution(ctx context.Context, exec *domain.JobExecution) error

	// Lifecycle
	Close() error
}
