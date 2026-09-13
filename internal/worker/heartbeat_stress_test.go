package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
	"github.com/AnkitxRot/ForgeFlow/internal/worker"
)

// mockStoreForHeartbeat implements a minimal store to simulate various database behaviors
type mockStoreForHeartbeat struct {
	store.Store
	renewErr   atomic.Pointer[error]
	renewCalls atomic.Int64
}

func (m *mockStoreForHeartbeat) RenewLease(ctx context.Context, tenantID, id string, fencingGen int64, leaseToken string, duration time.Duration) error {
	m.renewCalls.Add(1)
	if errPtr := m.renewErr.Load(); errPtr != nil {
		return *errPtr
	}
	return nil
}

func TestHeartbeat_NormalRenewal(t *testing.T) {
	ms := &mockStoreForHeartbeat{}
	token := "token-normal"
	job := &domain.Job{
		ID:                "job-normal",
		TenantID:          "tenant-1",
		FencingGeneration: 1,
		LeaseToken:        &token,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maintainer := worker.NewLeaseMaintainer(ms, job, 60*time.Millisecond, 20*time.Millisecond, cancel)
	maintainer.Start(ctx)

	time.Sleep(75 * time.Millisecond)
	maintainer.Stop()

	if ms.renewCalls.Load() < 2 {
		t.Fatalf("expected at least 2 renewals, got %d", ms.renewCalls.Load())
	}
	if maintainer.IsLeaseLost() {
		t.Fatal("lease should not be marked lost on successful renewals")
	}
}

func TestHeartbeat_StoreOutage_TriggersCancellation(t *testing.T) {
	ms := &mockStoreForHeartbeat{}
	errDB := errors.New("connection reset by peer")
	ms.renewErr.Store(&errDB)

	token := "token-outage"
	job := &domain.Job{
		ID:                "job-outage",
		TenantID:          "tenant-1",
		FencingGeneration: 1,
		LeaseToken:        &token,
	}

	cancelled := make(chan struct{})
	cancelJob := func() {
		close(cancelled)
	}

	maintainer := worker.NewLeaseMaintainer(ms, job, 60*time.Millisecond, 15*time.Millisecond, cancelJob)
	maintainer.Start(context.Background())
	defer maintainer.Stop()

	select {
	case <-cancelled:
		if !maintainer.IsLeaseLost() {
			t.Fatal("expected maintainer to record lease lost on store failure")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for heartbeat cancellation on database outage")
	}
}

func TestHeartbeat_LeaseAlreadyLost_TriggersCancellation(t *testing.T) {
	ms := &mockStoreForHeartbeat{}
	errLost := store.ErrLeaseLost
	ms.renewErr.Store(&errLost)

	token := "token-stale"
	job := &domain.Job{
		ID:                "job-stale",
		TenantID:          "tenant-1",
		FencingGeneration: 1,
		LeaseToken:        &token,
	}

	cancelled := make(chan struct{})
	cancelJob := func() {
		close(cancelled)
	}

	maintainer := worker.NewLeaseMaintainer(ms, job, 60*time.Millisecond, 15*time.Millisecond, cancelJob)
	maintainer.Start(context.Background())
	defer maintainer.Stop()

	select {
	case <-cancelled:
		if !maintainer.IsLeaseLost() {
			t.Fatal("expected maintainer to record lease lost on ErrLeaseLost")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for heartbeat cancellation on ErrLeaseLost")
	}
}

func TestHeartbeat_ParentContextCancellation(t *testing.T) {
	ms := &mockStoreForHeartbeat{}
	token := "token-parent-cancel"
	job := &domain.Job{
		ID:                "job-parent-cancel",
		TenantID:          "tenant-1",
		FencingGeneration: 1,
		LeaseToken:        &token,
	}

	ctx, cancel := context.WithCancel(context.Background())
	maintainer := worker.NewLeaseMaintainer(ms, job, 60*time.Millisecond, 15*time.Millisecond, func() {})
	maintainer.Start(ctx)

	// Cancel parent context
	cancel()

	// Maintainer should terminate cleanly
	done := make(chan struct{})
	go func() {
		maintainer.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Succeeded
	case <-time.After(1 * time.Second):
		t.Fatal("maintainer did not stop cleanly on parent context cancellation")
	}
}

func TestHeartbeat_RepeatedStartupShutdownCycle(t *testing.T) {
	ms := &mockStoreForHeartbeat{}
	token := "token-cycle"
	job := &domain.Job{
		ID:                "job-cycle",
		TenantID:          "tenant-1",
		FencingGeneration: 1,
		LeaseToken:        &token,
	}

	// 50 rapid sequential startup/shutdown cycles to detect race conditions or leaks
	for i := 0; i < 50; i++ {
		m := worker.NewLeaseMaintainer(ms, job, 50*time.Millisecond, 10*time.Millisecond, func() {})
		m.Start(context.Background())
		time.Sleep(2 * time.Millisecond)
		m.Stop()
		// Double stop should be safe
		m.Stop()
	}
}
