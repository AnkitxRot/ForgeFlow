package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
)

var (
	ErrNoHandlerRegistered = errors.New("no handler registered for job")
	ErrExecutionCancelled  = errors.New("job execution was cancelled")
)

// NonRetryableError marks an error as terminal; the worker should fail the job
// without scheduling further retries regardless of max_retries.
type NonRetryableError struct {
	Err error
}

func (e *NonRetryableError) Error() string {
	return e.Err.Error()
}

func (e *NonRetryableError) Unwrap() error {
	return e.Err
}

// MarkNonRetryable wraps an error in a NonRetryableError.
func MarkNonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &NonRetryableError{Err: err}
}

// IsNonRetryable checks if an error is explicitly marked non-retryable.
func IsNonRetryable(err error) bool {
	var nre *NonRetryableError
	return errors.As(err, &nre)
}

// Handler defines the interface for executing a claimed ForgeFlow job.
type Handler interface {
	Execute(ctx context.Context, job *domain.Job) ([]byte, error)
}

// HandlerFunc adapts a plain function to the Handler interface.
type HandlerFunc func(ctx context.Context, job *domain.Job) ([]byte, error)

func (f HandlerFunc) Execute(ctx context.Context, job *domain.Job) ([]byte, error) {
	return f(ctx, job)
}

// HandlerRegistry provides thread-safe dispatch of jobs to registered handlers.
// Dispatch order:
// 1. Explicit job type specified in payload ("type" or "handler" JSON key)
// 2. Queue name handler
// 3. Fallback default handler (if set)
type HandlerRegistry struct {
	mu             sync.RWMutex
	typeHandlers   map[string]Handler
	queueHandlers  map[string]Handler
	defaultHandler Handler
}

// NewHandlerRegistry creates an initialized HandlerRegistry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		typeHandlers:  make(map[string]Handler),
		queueHandlers: make(map[string]Handler),
	}
}

// RegisterType registers a handler for an explicit job type.
func (r *HandlerRegistry) RegisterType(jobType string, handler Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.typeHandlers[jobType] = handler
}

// RegisterQueue registers a fallback handler for any job claimed from the specified queue.
func (r *HandlerRegistry) RegisterQueue(queueName string, handler Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queueHandlers[queueName] = handler
}

// SetDefault sets a global fallback handler.
func (r *HandlerRegistry) SetDefault(handler Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defaultHandler = handler
}

// Resolve identifies the best-matching handler for a given job.
func (r *HandlerRegistry) Resolve(job *domain.Job) (Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 1. Check payload for "type" or "handler" identifier
	if len(job.Payload) > 0 {
		var meta struct {
			Type    string `json:"type"`
			Handler string `json:"handler"`
		}
		if err := json.Unmarshal(job.Payload, &meta); err == nil {
			target := meta.Type
			if target == "" {
				target = meta.Handler
			}
			if target != "" {
				if h, ok := r.typeHandlers[target]; ok {
					return h, nil
				}
			}
		}
	}

	// 2. Check queue-level handler
	if h, ok := r.queueHandlers[job.QueueName]; ok {
		return h, nil
	}

	// 3. Fallback handler
	if r.defaultHandler != nil {
		return r.defaultHandler, nil
	}

	return nil, fmt.Errorf("%w for queue %q (job id: %s)", ErrNoHandlerRegistered, job.QueueName, job.ID)
}
