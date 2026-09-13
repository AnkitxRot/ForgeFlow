package reaper

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/store"
)

var (
	ErrReaperAlreadyRunning = errors.New("reaper is already running")
)

// Config configures a Reaper instance.
type Config struct {
	Store     store.Store
	TenantID  string // optional: if empty, sweeps across all tenants
	BatchSize int
	Interval  time.Duration
}

// Validate checks configuration invariants.
func (c *Config) Validate() error {
	if c.Store == nil {
		return errors.New("store must not be nil")
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.Interval <= 0 {
		c.Interval = 5 * time.Second
	}
	return nil
}

// Reaper identifies expired running job leases and re-queues them or times them out.
type Reaper struct {
	cfg Config

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// New creates and validates a new Reaper instance.
func New(cfg Config) (*Reaper, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Reaper{
		cfg:    cfg,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}, nil
}

// ReapOnce performs a single pass, finding and recovering expired jobs.
// Returns the count of expired jobs recovered or timed out.
func (r *Reaper) ReapOnce(ctx context.Context) (int, error) {
	return r.cfg.Store.ReapExpiredJobs(ctx, r.cfg.TenantID, r.cfg.BatchSize)
}

// Start launches the periodic reaping loop in the background.
func (r *Reaper) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return ErrReaperAlreadyRunning
	}
	r.running = true
	r.stopCh = make(chan struct{})
	r.doneCh = make(chan struct{})
	r.mu.Unlock()

	go r.run(ctx)
	return nil
}

func (r *Reaper) run(ctx context.Context) {
	defer close(r.doneCh)

	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = r.ReapOnce(ctx)
		}
	}
}

// Stop terminates the background reaping loop and waits for any in-flight pass to finish.
func (r *Reaper) Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	close(r.stopCh)
	r.mu.Unlock()

	<-r.doneCh
}
