package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
	"github.com/google/uuid"
)

var (
	ErrWorkerNotRunning   = errors.New("worker is not in a running state")
	ErrWorkerAlreadyStart = errors.New("worker has already started")
	ErrDrainTimeout       = errors.New("timed out waiting for active jobs to drain")
)

// Config configures a Worker instance.
type Config struct {
	ID                string
	TenantID          string
	Queues            []string
	Concurrency       int
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	DrainTimeout      time.Duration
	Store             store.Store
	Registry          *HandlerRegistry
}

// Validate checks configuration invariants.
func (c *Config) Validate() error {
	if c.TenantID == "" {
		return errors.New("tenant ID must not be empty")
	}
	if len(c.Queues) == 0 {
		return errors.New("worker must subscribe to at least one queue")
	}
	if c.Store == nil {
		return errors.New("store must not be nil")
	}
	if c.Registry == nil {
		return errors.New("handler registry must not be nil")
	}
	if c.ID == "" {
		c.ID = fmt.Sprintf("worker-%s", uuid.New().String()[:8])
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 5
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 200 * time.Millisecond
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = c.LeaseDuration / 3
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 30 * time.Second
	}
	return nil
}

// Worker orchestrates polling, claiming, heartbeat maintenance, and execution of jobs.
type Worker struct {
	cfg Config

	mu          sync.RWMutex
	state       State
	activeCount int32
	activeWG    sync.WaitGroup
	slotNotify  chan struct{}

	cancelRoot context.CancelFunc
	stopPollCh chan struct{}
	pollDoneCh chan struct{}
}

// NewWorker validates the configuration and instantiates a Worker.
func NewWorker(cfg Config) (*Worker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid worker config: %w", err)
	}

	return &Worker{
		cfg:        cfg,
		state:      StateStarting,
		slotNotify: make(chan struct{}, cfg.Concurrency),
		stopPollCh: make(chan struct{}),
		pollDoneCh: make(chan struct{}),
	}, nil
}

// State returns the current lifecycle state.
func (w *Worker) State() State {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.state
}

// ActiveJobs returns the number of jobs currently executing.
func (w *Worker) ActiveJobs() int {
	return int(atomic.LoadInt32(&w.activeCount))
}

// Start initiates the worker's polling and execution engine.
func (w *Worker) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.state != StateStarting {
		w.mu.Unlock()
		return ErrWorkerAlreadyStart
	}
	if err := ValidateTransition(w.state, StateRunning); err != nil {
		w.mu.Unlock()
		return err
	}
	w.state = StateRunning

	rootCtx, cancelRoot := context.WithCancel(ctx)
	w.cancelRoot = cancelRoot
	w.mu.Unlock()

	go w.pollLoop(rootCtx)
	return nil
}

// Drain initiates graceful shutdown: stops polling for new work and waits for active jobs to complete.
func (w *Worker) Drain(ctx context.Context) error {
	w.mu.Lock()
	if w.state == StateStopped {
		w.mu.Unlock()
		return nil
	}
	if w.state == StateDraining {
		w.mu.Unlock()
		// Already draining, wait on existing shutdown
		return w.waitForDrain(ctx)
	}
	if err := ValidateTransition(w.state, StateDraining); err != nil {
		w.mu.Unlock()
		return err
	}
	w.state = StateDraining
	close(w.stopPollCh)
	w.mu.Unlock()

	// Wait for poller to exit
	<-w.pollDoneCh

	return w.waitForDrain(ctx)
}

func (w *Worker) waitForDrain(ctx context.Context) error {
	drainDone := make(chan struct{})
	go func() {
		w.activeWG.Wait()
		close(drainDone)
	}()

	select {
	case <-drainDone:
		w.mu.Lock()
		w.state = StateStopped
		if w.cancelRoot != nil {
			w.cancelRoot()
		}
		w.mu.Unlock()
		return nil
	case <-ctx.Done():
		// Force cancellation of active executions
		w.mu.Lock()
		w.state = StateStopped
		if w.cancelRoot != nil {
			w.cancelRoot()
		}
		w.mu.Unlock()
		return fmt.Errorf("%w: %v", ErrDrainTimeout, ctx.Err())
	}
}

// Stop wraps Drain with the configured DrainTimeout.
func (w *Worker) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.DrainTimeout)
	defer cancel()
	return w.Drain(ctx)
}

func (w *Worker) pollLoop(ctx context.Context) {
	defer close(w.pollDoneCh)

	for {
		w.mu.RLock()
		canClaim := w.state.CanClaim()
		w.mu.RUnlock()

		if !canClaim {
			return
		}

		// Calculate available capacity
		currentActive := int(atomic.LoadInt32(&w.activeCount))
		avail := w.cfg.Concurrency - currentActive
		if avail <= 0 {
			// Wait for an execution slot to free up or stop signal
			select {
			case <-w.stopPollCh:
				return
			case <-ctx.Done():
				return
			case <-w.slotNotify:
				continue
			case <-time.After(w.cfg.PollInterval):
				continue
			}
		}

		// Atomically claim jobs up to available capacity
		claimed, err := w.cfg.Store.ClaimJobs(
			ctx,
			w.cfg.TenantID,
			w.cfg.ID,
			w.cfg.Queues,
			avail,
			w.cfg.LeaseDuration,
		)
		if err != nil {
			// On store error or context cancellation, sleep briefly
			select {
			case <-w.stopPollCh:
				return
			case <-ctx.Done():
				return
			case <-time.After(w.cfg.PollInterval):
				continue
			}
		}

		if len(claimed) == 0 {
			// No eligible work in queue
			select {
			case <-w.stopPollCh:
				return
			case <-ctx.Done():
				return
			case <-time.After(w.cfg.PollInterval):
				continue
			}
		}

		// Dispatch claimed jobs
		for _, job := range claimed {
			atomic.AddInt32(&w.activeCount, 1)
			w.activeWG.Add(1)
			go w.executeJob(ctx, job)
		}
	}
}

func (w *Worker) executeJob(rootCtx context.Context, job *domain.Job) {
	defer func() {
		atomic.AddInt32(&w.activeCount, -1)
		w.activeWG.Done()
		// Signal poller that a slot is free
		select {
		case w.slotNotify <- struct{}{}:
		default:
		}
	}()

	// 1. Resolve Handler
	handler, err := w.cfg.Registry.Resolve(job)
	if err != nil {
		// No handler registered: terminal failure without retry
		if job.LeaseToken != nil {
			_ = w.cfg.Store.FailJob(
				context.Background(),
				job.TenantID,
				job.ID,
				job.FencingGeneration,
				*job.LeaseToken,
				err.Error(),
				false, // not retryable
				0,
			)
		}
		return
	}

	// 2. Setup job context with timeout and cancellation
	var jobCtx context.Context
	var cancel context.CancelFunc
	if job.TimeoutSeconds > 0 {
		jobCtx, cancel = context.WithTimeout(rootCtx, time.Duration(job.TimeoutSeconds)*time.Second)
	} else {
		jobCtx, cancel = context.WithCancel(rootCtx)
	}
	defer cancel()

	// 3. Start Lease Maintainer (heartbeat)
	maintainer := NewLeaseMaintainer(
		w.cfg.Store,
		job,
		w.cfg.LeaseDuration,
		w.cfg.HeartbeatInterval,
		cancel,
	)
	maintainer.Start(rootCtx)

	// 4. Execute Job Handler
	result, execErr := handler.Execute(jobCtx, job)

	// 5. Stop Lease Maintainer and inspect heartbeat outcome
	maintainer.Stop()

	// If lease was lost during execution, DO NOT attempt to mutate state
	if maintainer.IsLeaseLost() {
		return
	}

	// If context was cancelled by root shutdown or lease loss
	if jobCtx.Err() != nil && execErr == nil {
		execErr = jobCtx.Err()
	}

	// 6. Report Completion or Failure
	if job.LeaseToken == nil {
		return
	}

	completionCtx := context.Background()

	if execErr == nil {
		_ = w.cfg.Store.CompleteJob(
			completionCtx,
			job.TenantID,
			job.ID,
			job.FencingGeneration,
			*job.LeaseToken,
			result,
		)
	} else {
		retryable := !IsNonRetryable(execErr)
		backoff := time.Duration(job.RetryBackoffSeconds) * time.Second
		if backoff <= 0 {
			backoff = 5 * time.Second
		}
		_ = w.cfg.Store.FailJob(
			completionCtx,
			job.TenantID,
			job.ID,
			job.FencingGeneration,
			*job.LeaseToken,
			execErr.Error(),
			retryable,
			backoff,
		)
	}
}
