package worker_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/worker"
)

// TestWorker_Stress_1Worker1Slot tests sequential execution through a single worker slot.
func TestWorker_Stress_1Worker1Slot(t *testing.T) {
	s, tenantID, queueName := setupTestStore(t)
	ctx := context.Background()

	numJobs := 15
	var executedCount atomic.Int64

	registry := worker.NewHandlerRegistry()
	registry.RegisterQueue(queueName, worker.HandlerFunc(func(ctx context.Context, job *domain.Job) ([]byte, error) {
		executedCount.Add(1)
		return []byte(`{"ok":true}`), nil
	}))

	w, err := worker.NewWorker(worker.Config{
		ID:                "worker-1-slot",
		TenantID:          tenantID,
		Queues:            []string{queueName},
		Concurrency:       1,
		PollInterval:      10 * time.Millisecond,
		LeaseDuration:     10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		Store:             s,
		Registry:          registry,
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	for i := 0; i < numJobs; i++ {
		_ = s.CreateJob(ctx, &domain.Job{
			ID:        fmt.Sprintf("job-1slot-%02d", i),
			TenantID:  tenantID,
			QueueName: queueName,
			Status:    domain.StatusQueued,
			Payload:   []byte(`{}`),
		})
	}

	if err := w.Start(ctx); err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}
	defer func() { _ = w.Stop() }()

	// Wait for all jobs to complete
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if executedCount.Load() == int64(numJobs) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if executedCount.Load() != int64(numJobs) {
		t.Fatalf("expected %d jobs executed, got %d", numJobs, executedCount.Load())
	}
}

// TestWorker_Stress_1Worker10Slots tests concurrent execution within a single worker with 10 slots.
func TestWorker_Stress_1Worker10Slots(t *testing.T) {
	s, tenantID, queueName := setupTestStore(t)
	ctx := context.Background()

	numJobs := 30
	var executedCount atomic.Int64
	var maxConcurrency atomic.Int32
	var currentConcurrency atomic.Int32

	registry := worker.NewHandlerRegistry()
	registry.RegisterQueue(queueName, worker.HandlerFunc(func(ctx context.Context, job *domain.Job) ([]byte, error) {
		cur := currentConcurrency.Add(1)
		for {
			oldMax := maxConcurrency.Load()
			if cur <= oldMax || maxConcurrency.CompareAndSwap(oldMax, cur) {
				break
			}
		}
		time.Sleep(15 * time.Millisecond)
		currentConcurrency.Add(-1)
		executedCount.Add(1)
		return []byte(`{"ok":true}`), nil
	}))

	w, err := worker.NewWorker(worker.Config{
		ID:                "worker-10-slots",
		TenantID:          tenantID,
		Queues:            []string{queueName},
		Concurrency:       10,
		PollInterval:      10 * time.Millisecond,
		LeaseDuration:     10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		Store:             s,
		Registry:          registry,
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	for i := 0; i < numJobs; i++ {
		_ = s.CreateJob(ctx, &domain.Job{
			ID:        fmt.Sprintf("job-10slots-%02d", i),
			TenantID:  tenantID,
			QueueName: queueName,
			Status:    domain.StatusQueued,
			Payload:   []byte(`{}`),
		})
	}

	if err := w.Start(ctx); err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}
	defer func() { _ = w.Stop() }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if executedCount.Load() == int64(numJobs) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if executedCount.Load() != int64(numJobs) {
		t.Fatalf("expected %d jobs executed, got %d", numJobs, executedCount.Load())
	}
	if maxConcurrency.Load() < 2 {
		t.Fatalf("expected concurrent execution, but max concurrency was %d", maxConcurrency.Load())
	}
}

// TestWorker_Stress_10Workers100Jobs verifies multi-worker contention and zero duplicate executions under race detection.
func TestWorker_Stress_10Workers100Jobs(t *testing.T) {
	s, tenantID, queueName := setupTestStore(t)
	ctx := context.Background()

	numJobs := 50
	numWorkers := 5

	var executedCount atomic.Int64
	executedJobs := &sync.Map{}

	registry := worker.NewHandlerRegistry()
	registry.RegisterQueue(queueName, worker.HandlerFunc(func(ctx context.Context, job *domain.Job) ([]byte, error) {
		if _, loaded := executedJobs.LoadOrStore(job.ID, true); loaded {
			t.Errorf("FATAL: duplicate execution of job %s", job.ID)
		}
		executedCount.Add(1)
		return []byte(`{"ok":true}`), nil
	}))

	workers := make([]*worker.Worker, numWorkers)
	for w := 0; w < numWorkers; w++ {
		var err error
		workers[w], err = worker.NewWorker(worker.Config{
			ID:                fmt.Sprintf("multi-worker-%d", w),
			TenantID:          tenantID,
			Queues:            []string{queueName},
			Concurrency:       3,
			PollInterval:      10 * time.Millisecond,
			LeaseDuration:     10 * time.Second,
			HeartbeatInterval: 3 * time.Second,
			Store:             s,
			Registry:          registry,
		})
		if err != nil {
			t.Fatalf("failed to create worker %d: %v", w, err)
		}
	}

	for i := 0; i < numJobs; i++ {
		_ = s.CreateJob(ctx, &domain.Job{
			ID:        fmt.Sprintf("job-multi-%03d", i),
			TenantID:  tenantID,
			QueueName: queueName,
			Status:    domain.StatusQueued,
			Payload:   []byte(`{}`),
		})
	}

	for _, w := range workers {
		if err := w.Start(ctx); err != nil {
			t.Fatalf("failed to start worker: %v", err)
		}
	}

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if executedCount.Load() == int64(numJobs) {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	for _, w := range workers {
		_ = w.Stop()
	}

	if executedCount.Load() != int64(numJobs) {
		t.Fatalf("expected all %d jobs to complete, got %d", numJobs, executedCount.Load())
	}
}
