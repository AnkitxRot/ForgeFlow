package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
)

// LeaseMaintainer manages the background periodic renewal of a job lease.
// If renewal fails (e.g. ErrLeaseLost), it cancels the job execution context immediately.
type LeaseMaintainer struct {
	store             store.Store
	job               *domain.Job
	leaseDuration     time.Duration
	heartbeatInterval time.Duration
	cancelJob         context.CancelFunc
	stopOnce          sync.Once
	stopCh            chan struct{}
	doneCh            chan struct{}
	leaseLost         int32 // atomic boolean (1 if lease was lost)
}

// NewLeaseMaintainer creates a LeaseMaintainer for an active job execution.
func NewLeaseMaintainer(
	s store.Store,
	job *domain.Job,
	leaseDuration time.Duration,
	heartbeatInterval time.Duration,
	cancelJob context.CancelFunc,
) *LeaseMaintainer {
	if heartbeatInterval <= 0 {
		heartbeatInterval = leaseDuration / 3
		if heartbeatInterval <= 0 {
			heartbeatInterval = 1 * time.Second
		}
	}

	return &LeaseMaintainer{
		store:             s,
		job:               job,
		leaseDuration:     leaseDuration,
		heartbeatInterval: heartbeatInterval,
		cancelJob:         cancelJob,
		stopCh:            make(chan struct{}),
		doneCh:            make(chan struct{}),
	}
}

// Start launches the renewal goroutine in the background.
func (m *LeaseMaintainer) Start(ctx context.Context) {
	go m.run(ctx)
}

func (m *LeaseMaintainer) run(ctx context.Context) {
	defer close(m.doneCh)

	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if m.job.LeaseToken == nil {
				return
			}
			// Attempt to renew lease
			err := m.store.RenewLease(
				ctx,
				m.job.TenantID,
				m.job.ID,
				m.job.FencingGeneration,
				*m.job.LeaseToken,
				m.leaseDuration,
			)
			if err != nil {
				// Fatal lease renewal failure: lease lost or store failure
				atomic.StoreInt32(&m.leaseLost, 1)
				// Immediate cancellation of active execution context
				m.cancelJob()
				return
			}
		}
	}
}

// Stop signals the maintainer to stop and blocks until the renewal goroutine finishes.
func (m *LeaseMaintainer) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
	<-m.doneCh
}

// IsLeaseLost returns true if the maintainer encountered a lease renewal failure.
func (m *LeaseMaintainer) IsLeaseLost() bool {
	return atomic.LoadInt32(&m.leaseLost) == 1
}
