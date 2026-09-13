package integration_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
)

// TestPostgres_Stress_CasesA_Through_D runs high-concurrency contention benchmarks:
// Case A: 1 job, 50 workers -> exactly 1 claim
// Case B: 1 job, 100 workers -> exactly 1 claim
// Case C: 10 jobs, 100 workers -> exactly 10 claims, 0 duplicates
// Case D: 100 jobs, 100 workers -> exactly 100 claims, 0 duplicates
func TestPostgres_Stress_CasesA_Through_D(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	cases := []struct {
		name       string
		numJobs    int
		numWorkers int
	}{
		{name: "CaseA_1Job_50Workers", numJobs: 1, numWorkers: 50},
		{name: "CaseB_1Job_100Workers", numJobs: 1, numWorkers: 100},
		{name: "CaseC_10Jobs_100Workers", numJobs: 10, numWorkers: 100},
		{name: "CaseD_100Jobs_100Workers", numJobs: 100, numWorkers: 100},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := "tenant-stress-" + uuid.NewString()[:8]
			queueName := "stress-q"
			setupTenantAndQueue(t, s, tenantID, queueName)

			// 1. Enqueue test jobs
			for j := 0; j < tc.numJobs; j++ {
				job := &domain.Job{
					ID:        fmt.Sprintf("job-%s-%04d", tenantID, j),
					TenantID:  tenantID,
					QueueName: queueName,
					Status:    domain.StatusQueued,
					Priority:  j % 10,
					Payload:   []byte(fmt.Sprintf(`{"index":%d}`, j)),
				}
				if err := s.CreateJob(ctx, job); err != nil {
					t.Fatalf("failed to create job %d: %v", j, err)
				}
			}

			// 2. Launch concurrent claiming workers with barrier synchronization
			var wg sync.WaitGroup
			wg.Add(tc.numWorkers)

			claimedMap := &sync.Map{}
			var totalClaimed atomic.Int64
			var claimErrors atomic.Int64

			startBarrier := make(chan struct{})
			startTime := time.Now()

			for w := 0; w < tc.numWorkers; w++ {
				wID := fmt.Sprintf("worker-%03d", w)
				go func(workerID string) {
					defer wg.Done()
					<-startBarrier

					claimCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()

					jobs, err := s.ClaimJobs(claimCtx, tenantID, workerID, []string{queueName}, 1, 30*time.Second)
					if err != nil {
						claimErrors.Add(1)
						return
					}

					for _, job := range jobs {
						totalClaimed.Add(1)
						// Verify no other worker claimed the same job ID
						if _, loaded := claimedMap.LoadOrStore(job.ID, workerID); loaded {
							t.Errorf("CRITICAL CONCURRENCY VIOLATION: duplicate claim on job %s by worker %s", job.ID, workerID)
						}
					}
				}(wID)
			}

			close(startBarrier)
			wg.Wait()
			duration := time.Since(startTime)

			t.Logf("[%s] workers=%d, jobs=%d -> claimed=%d, errors=%d, duration=%v",
				tc.name, tc.numWorkers, tc.numJobs, totalClaimed.Load(), claimErrors.Load(), duration)

			if claimErrors.Load() > 0 {
				t.Fatalf("encountered %d claim errors under contention", claimErrors.Load())
			}

			if int(totalClaimed.Load()) != tc.numJobs {
				t.Fatalf("expected exactly %d jobs claimed, got %d", tc.numJobs, totalClaimed.Load())
			}

			// 3. Directly inspect PostgreSQL durable state to rule out ghost or dual mutations
			for j := 0; j < tc.numJobs; j++ {
				jID := fmt.Sprintf("job-%s-%04d", tenantID, j)
				dj, err := s.GetJob(ctx, tenantID, jID)
				if err != nil {
					t.Fatalf("failed to read job %s from durable state: %v", jID, err)
				}
				if dj.Status != domain.StatusRunning {
					t.Fatalf("durable job %s has status %s, expected RUNNING", jID, dj.Status)
				}
				if dj.Attempt != 1 {
					t.Fatalf("durable job %s has attempt %d, expected 1", jID, dj.Attempt)
				}
				if dj.FencingGeneration != 1 {
					t.Fatalf("durable job %s has fencing_generation %d, expected 1", jID, dj.FencingGeneration)
				}
				if dj.LeaseToken == nil || *dj.LeaseToken == "" {
					t.Fatalf("durable job %s has nil or empty lease_token", jID)
				}
			}
		})
	}
}

// TestCaseE_MultipleQueues tests that workers subscribing to distinct queues
// claim only from their designated queues without cross-queue interference.
func TestPostgres_Stress_CaseE_MultipleQueues(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantID := "tenant-queues-" + uuid.NewString()[:8]
	queues := []string{"high-priority", "batch-processing", "notifications"}

	for _, q := range queues {
		setupTenantAndQueue(t, s, tenantID, q)
	}

	// Enqueue 20 jobs per queue
	jobsPerQueue := 20
	for _, q := range queues {
		for i := 0; i < jobsPerQueue; i++ {
			job := &domain.Job{
				ID:        fmt.Sprintf("job-%s-%s-%03d", tenantID, q, i),
				TenantID:  tenantID,
				QueueName: q,
				Status:    domain.StatusQueued,
				Payload:   []byte(`{"queue_test":true}`),
			}
			if err := s.CreateJob(ctx, job); err != nil {
				t.Fatalf("failed to create job: %v", err)
			}
		}
	}

	// 30 workers, 10 dedicated per queue
	var wg sync.WaitGroup
	workersPerQueue := 10
	wg.Add(len(queues) * workersPerQueue)

	var claimedCount atomic.Int64
	for _, q := range queues {
		for w := 0; w < workersPerQueue; w++ {
			targetQueue := q
			wID := fmt.Sprintf("worker-%s-%d", targetQueue, w)
			go func(workerID, qName string) {
				defer wg.Done()
				for {
					jobs, err := s.ClaimJobs(ctx, tenantID, workerID, []string{qName}, 2, 30*time.Second)
					if err != nil || len(jobs) == 0 {
						break
					}
					for _, j := range jobs {
						if j.QueueName != qName {
							t.Errorf("worker claimed from wrong queue: expected %s, got %s", qName, j.QueueName)
						}
						claimedCount.Add(1)
					}
				}
			}(wID, targetQueue)
		}
	}

	wg.Wait()
	expectedTotal := len(queues) * jobsPerQueue
	if int(claimedCount.Load()) != expectedTotal {
		t.Fatalf("expected total %d jobs claimed across queues, got %d", expectedTotal, claimedCount.Load())
	}

	// Verify durable state in PostgreSQL
	for _, q := range queues {
		for i := 0; i < jobsPerQueue; i++ {
			jID := fmt.Sprintf("job-%s-%s-%03d", tenantID, q, i)
			dj, err := s.GetJob(ctx, tenantID, jID)
			if err != nil || dj.Status != domain.StatusRunning {
				t.Fatalf("durable job %s has status %v (err=%v)", jID, dj.Status, err)
			}
		}
	}
}

// TestCaseF_MultipleTenants tests concurrent claiming across multiple isolated tenants
// ensuring no cross-tenant leakage under concurrent database load.
func TestPostgres_Stress_CaseF_MultipleTenants(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	numTenants := 5
	jobsPerTenant := 15
	queueName := "tenant-shared-queue"

	tenants := make([]string, numTenants)
	for i := 0; i < numTenants; i++ {
		tenants[i] = fmt.Sprintf("tenant-%s-%d", uuid.NewString()[:8], i)
		setupTenantAndQueue(t, s, tenants[i], queueName)

		for j := 0; j < jobsPerTenant; j++ {
			job := &domain.Job{
				ID:        fmt.Sprintf("job-%s-%03d", tenants[i], j),
				TenantID:  tenants[i],
				QueueName: queueName,
				Status:    domain.StatusQueued,
				Payload:   []byte(`{}`),
			}
			if err := s.CreateJob(ctx, job); err != nil {
				t.Fatalf("failed to create job: %v", err)
			}
		}
	}

	var wg sync.WaitGroup
	var totalClaims atomic.Int64
	wg.Add(numTenants * 4) // 4 workers per tenant

	for _, tID := range tenants {
		for w := 0; w < 4; w++ {
			curTenant := tID
			workerID := fmt.Sprintf("worker-%s-%d", curTenant, w)
			go func(tenant, worker string) {
				defer wg.Done()
				for {
					jobs, err := s.ClaimJobs(ctx, tenant, worker, []string{queueName}, 2, 30*time.Second)
					if err != nil || len(jobs) == 0 {
						break
					}
					for _, j := range jobs {
						if j.TenantID != tenant {
							t.Errorf("SECURITY FAULT: cross-tenant claim! expected %s, got %s", tenant, j.TenantID)
						}
						totalClaims.Add(1)
					}
				}
			}(curTenant, workerID)
		}
	}

	wg.Wait()
	expectedTotal := numTenants * jobsPerTenant
	if int(totalClaims.Load()) != expectedTotal {
		t.Fatalf("expected %d total tenant claims, got %d", expectedTotal, totalClaims.Load())
	}

	// Verify durable tenant isolation in PostgreSQL
	for _, tID := range tenants {
		for j := 0; j < jobsPerTenant; j++ {
			jID := fmt.Sprintf("job-%s-%03d", tID, j)
			dj, err := s.GetJob(ctx, tID, jID)
			if err != nil || dj.Status != domain.StatusRunning {
				t.Fatalf("durable job %s has status %v (err=%v)", jID, dj.Status, err)
			}
		}
	}
}

// TestCaseG_BatchClaims verifies atomic claiming of batches (e.g. batch size 5, 10).
func TestPostgres_Stress_CaseG_BatchClaims(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantID := "tenant-batch-" + uuid.NewString()[:8]
	queueName := "batch-q"
	setupTenantAndQueue(t, s, tenantID, queueName)

	totalJobs := 50
	for i := 0; i < totalJobs; i++ {
		job := &domain.Job{
			ID:        fmt.Sprintf("batch-job-%s-%03d", tenantID, i),
			TenantID:  tenantID,
			QueueName: queueName,
			Status:    domain.StatusQueued,
			Payload:   []byte(`{"batch":true}`),
		}
		if err := s.CreateJob(ctx, job); err != nil {
			t.Fatalf("failed to create job: %v", err)
		}
	}

	// 5 workers claiming batch of 10 concurrently
	var wg sync.WaitGroup
	workers := 5
	wg.Add(workers)

	var claimedCount atomic.Int64
	claimedMap := &sync.Map{}

	for w := 0; w < workers; w++ {
		workerID := fmt.Sprintf("batch-worker-%d", w)
		go func(wID string) {
			defer wg.Done()
			jobs, err := s.ClaimJobs(ctx, tenantID, wID, []string{queueName}, 10, 30*time.Second)
			if err != nil {
				t.Errorf("batch claim error: %v", err)
				return
			}
			for _, j := range jobs {
				claimedCount.Add(1)
				if _, exists := claimedMap.LoadOrStore(j.ID, wID); exists {
					t.Errorf("duplicate claim detected on job %s", j.ID)
				}
			}
		}(workerID)
	}

	wg.Wait()
	if int(claimedCount.Load()) != totalJobs {
		t.Fatalf("expected all %d jobs claimed in batches, got %d", totalJobs, claimedCount.Load())
	}

	// Verify durable state in PostgreSQL
	for i := 0; i < totalJobs; i++ {
		jID := fmt.Sprintf("batch-job-%s-%03d", tenantID, i)
		dj, err := s.GetJob(ctx, tenantID, jID)
		if err != nil || dj.Status != domain.StatusRunning || dj.Attempt != 1 || dj.FencingGeneration != 1 {
			t.Fatalf("durable job %s invalid state: status=%s, attempt=%d, gen=%d", jID, dj.Status, dj.Attempt, dj.FencingGeneration)
		}
	}
}

// TestCaseH_ContinuousClaims verifies workers repeatedly claiming as jobs become available.
func TestPostgres_Stress_CaseH_ContinuousClaims(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tenantID := "tenant-cont-" + uuid.NewString()[:8]
	queueName := "continuous-q"
	setupTenantAndQueue(t, s, tenantID, queueName)

	totalProduced := 60
	var totalConsumed atomic.Int64

	// Producer goroutine enqueues jobs in waves
	go func() {
		for i := 0; i < totalProduced; i++ {
			job := &domain.Job{
				ID:        fmt.Sprintf("cont-job-%s-%03d", tenantID, i),
				TenantID:  tenantID,
				QueueName: queueName,
				Status:    domain.StatusQueued,
				Payload:   []byte(`{}`),
			}
			_ = s.CreateJob(ctx, job)
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// 8 consumer workers repeatedly claiming and completing
	var wg sync.WaitGroup
	workers := 8
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		workerID := fmt.Sprintf("continuous-worker-%d", w)
		go func(wID string) {
			defer wg.Done()
			for {
				if totalConsumed.Load() >= int64(totalProduced) {
					return
				}
				jobs, err := s.ClaimJobs(ctx, tenantID, wID, []string{queueName}, 2, 10*time.Second)
				if err != nil {
					return
				}
				for _, j := range jobs {
					_ = s.CompleteJob(ctx, tenantID, j.ID, j.FencingGeneration, *j.LeaseToken, []byte(`{"done":true}`))
					totalConsumed.Add(1)
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(workerID)
	}

	wg.Wait()
	if totalConsumed.Load() < int64(totalProduced) {
		t.Fatalf("expected %d consumed jobs, got %d", totalProduced, totalConsumed.Load())
	}
}
