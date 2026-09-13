package tests_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/retry"
	"github.com/AnkitxRot/ForgeFlow/internal/store/postgres"
	"github.com/AnkitxRot/ForgeFlow/internal/store/sqlite"
	"github.com/AnkitxRot/ForgeFlow/internal/workflow"
)

// BenchmarkValidateDAG benchmarks DAG static validation and topological sorting across 10, 100, and 1000 nodes.
func BenchmarkValidateDAG(b *testing.B) {
	nodeCounts := []int{10, 100, 1000}

	for _, n := range nodeCounts {
		b.Run(fmt.Sprintf("Nodes-%d", n), func(b *testing.B) {
			steps := make([]workflow.StepDefinition, n)
			for i := 0; i < n; i++ {
				var deps []string
				if i > 0 {
					deps = []string{fmt.Sprintf("step-%d", i-1)}
				}
				steps[i] = workflow.StepDefinition{
					Name:         fmt.Sprintf("step-%d", i),
					Handler:      "test-handler",
					Dependencies: deps,
				}
			}
			def := &workflow.Definition{
				Name:  fmt.Sprintf("dag-benchmark-%d", n),
				Steps: steps,
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := workflow.ValidateDAG(def)
				if err != nil {
					b.Fatalf("unexpected validation failure: %v", err)
				}
			}
		})
	}
}

// BenchmarkResolveTemplate benchmarks expression resolution across 100 variable lookups.
func BenchmarkResolveTemplate(b *testing.B) {
	ctx := workflow.PipeContext{}
	tmplMap := make(map[string]string)

	for i := 0; i < 100; i++ {
		stepName := fmt.Sprintf("step_%d", i)
		ctx[stepName] = []byte(fmt.Sprintf(`{"field_val":"token-value-%d"}`, i))
		tmplMap[fmt.Sprintf("key_%d", i)] = fmt.Sprintf("{{steps.%s.output.field_val}}", stepName)
	}

	rawTmpl, err := json.Marshal(tmplMap)
	if err != nil {
		b.Fatalf("failed to marshal template: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := workflow.ResolveTemplate(rawTmpl, ctx)
		if err != nil {
			b.Fatalf("unexpected template resolution error: %v", err)
		}
		if len(res) == 0 {
			b.Fatalf("empty resolved result")
		}
	}
}

// BenchmarkComputeBackoff benchmarks backoff calculation across various policies and jitter settings.
func BenchmarkComputeBackoff(b *testing.B) {
	policy := retry.NewPolicy(100*time.Millisecond, 10*time.Second, 2.0, retry.JitterFull)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = policy.ComputeBackoff((i % 10) + 1)
	}
}

// BenchmarkSQLiteClaimAndComplete benchmarks the end-to-end claim and complete cycle in SQLite.
func BenchmarkSQLiteClaimAndComplete(b *testing.B) {
	tempDir := b.TempDir()
	s, err := sqlite.Open(filepath.Join(tempDir, "bench_claim_complete.db"))
	if err != nil {
		b.Fatalf("failed to open sqlite store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	tenantID := "bench-tenant"
	queueName := "bench-queue"

	_ = s.CreateTenant(ctx, &domain.Tenant{ID: tenantID, Name: "Benchmark Tenant"})
	_ = s.CreateQueue(ctx, &domain.Queue{TenantID: tenantID, Name: queueName})

	// Pre-enqueue b.N jobs before timing the claim-and-complete loop
	for i := 0; i < b.N; i++ {
		job := &domain.Job{
			ID:        fmt.Sprintf("bench-job-%d", i),
			TenantID:  tenantID,
			QueueName: queueName,
			Status:    domain.StatusQueued,
			Payload:   []byte(`{"benchmark":true}`),
		}
		if err := s.CreateJob(ctx, job); err != nil {
			b.Fatalf("failed to enqueue: %v", err)
		}
	}

	workerID := "bench-worker"
	output := []byte(`{"status":"completed"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jobs, err := s.ClaimJobs(ctx, tenantID, workerID, []string{queueName}, 1, 30*time.Second)
		if err != nil {
			b.Fatalf("claim failed at iteration %d: %v", i, err)
		}
		if len(jobs) == 0 {
			b.Fatalf("no jobs claimed at iteration %d", i)
		}
		job := jobs[0]
		if err := s.CompleteJob(ctx, tenantID, job.ID, job.FencingGeneration, *job.LeaseToken, output); err != nil {
			b.Fatalf("complete failed at iteration %d: %v", i, err)
		}
	}
}

func getBenchPostgresStore(b *testing.B) *postgres.PostgresStore {
	b.Helper()
	connStr := os.Getenv("TEST_POSTGRES_URL")
	if connStr == "" {
		b.Skip("skipping postgres benchmark: TEST_POSTGRES_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s, err := postgres.Open(ctx, connStr)
	if err != nil {
		b.Fatalf("failed to open postgres: %v", err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s
}

// BenchmarkPostgresClaimAndComplete benchmarks atomic claim + complete in PostgreSQL.
func BenchmarkPostgresClaimAndComplete(b *testing.B) {
	s := getBenchPostgresStore(b)
	ctx := context.Background()

	runID := uuid.NewString()[:8]
	tenantID := fmt.Sprintf("tenant-bench-pg-%s-%d", runID, b.N)
	queueName := "pg-bench-q"
	_ = s.CreateTenant(ctx, &domain.Tenant{ID: tenantID, Name: "PG Bench"})
	_ = s.CreateQueue(ctx, &domain.Queue{TenantID: tenantID, Name: queueName})

	for i := 0; i < b.N; i++ {
		job := &domain.Job{
			ID:        fmt.Sprintf("job-pg-%s-%d", runID, i),
			TenantID:  tenantID,
			QueueName: queueName,
			Status:    domain.StatusQueued,
			Payload:   []byte(`{"pg_bench":true}`),
		}
		if err := s.CreateJob(ctx, job); err != nil {
			b.Fatalf("failed to enqueue: %v", err)
		}
	}

	workerID := "worker-pg-bench"
	output := []byte(`{"status":"done"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jobs, err := s.ClaimJobs(ctx, tenantID, workerID, []string{queueName}, 1, 30*time.Second)
		if err != nil || len(jobs) == 0 {
			b.Fatalf("claim failed at %d: %v", i, err)
		}
		job := jobs[0]
		if err := s.CompleteJob(ctx, tenantID, job.ID, job.FencingGeneration, *job.LeaseToken, output); err != nil {
			b.Fatalf("complete failed at %d: %v", i, err)
		}
	}
}

// BenchmarkPostgresRenewLease benchmarks lease renewal roundtrip in PostgreSQL.
func BenchmarkPostgresRenewLease(b *testing.B) {
	s := getBenchPostgresStore(b)
	ctx := context.Background()

	runID := uuid.NewString()[:8]
	tenantID := fmt.Sprintf("tenant-bench-renew-%s-%d", runID, b.N)
	queueName := "pg-renew-q"
	_ = s.CreateTenant(ctx, &domain.Tenant{ID: tenantID, Name: "Renew Bench"})
	_ = s.CreateQueue(ctx, &domain.Queue{TenantID: tenantID, Name: queueName})

	job := &domain.Job{
		ID:        fmt.Sprintf("job-renew-%s", runID),
		TenantID:  tenantID,
		QueueName: queueName,
		Status:    domain.StatusQueued,
		Payload:   []byte(`{}`),
	}
	if err := s.CreateJob(ctx, job); err != nil {
		b.Fatalf("create job failed: %v", err)
	}

	claimed, err := s.ClaimJobs(ctx, tenantID, "renew-worker", []string{queueName}, 1, 30*time.Second)
	if err != nil || len(claimed) != 1 {
		b.Fatalf("claim failed: %v", err)
	}
	cJob := claimed[0]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.RenewLease(ctx, tenantID, cJob.ID, cJob.FencingGeneration, *cJob.LeaseToken, 30*time.Second); err != nil {
			b.Fatalf("renew failed at %d: %v", i, err)
		}
	}
}

// BenchmarkPostgresConcurrentClaim benchmarks multi-worker contention throughput in PostgreSQL.
func BenchmarkPostgresConcurrentClaim(b *testing.B) {
	s := getBenchPostgresStore(b)
	ctx := context.Background()

	tenantID := "tenant-bench-conc-" + uuid.NewString()[:8]
	queueName := "pg-conc-q"
	_ = s.CreateTenant(ctx, &domain.Tenant{ID: tenantID, Name: "Conc Bench"})
	_ = s.CreateQueue(ctx, &domain.Queue{TenantID: tenantID, Name: queueName})

	for i := 0; i < b.N; i++ {
		_ = s.CreateJob(ctx, &domain.Job{
			ID:        fmt.Sprintf("job-conc-%d", i),
			TenantID:  tenantID,
			QueueName: queueName,
			Status:    domain.StatusQueued,
			Payload:   []byte(`{}`),
		})
	}

	numWorkers := 8
	jobsPerWorker := b.N / numWorkers
	if jobsPerWorker == 0 {
		jobsPerWorker = 1
	}

	b.ResetTimer()
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		workerID := fmt.Sprintf("worker-bench-%d", w)
		go func(wID string) {
			defer wg.Done()
			for {
				jobs, err := s.ClaimJobs(ctx, tenantID, wID, []string{queueName}, 1, 30*time.Second)
				if err != nil || len(jobs) == 0 {
					break
				}
				_ = s.CompleteJob(ctx, tenantID, jobs[0].ID, jobs[0].FencingGeneration, *jobs[0].LeaseToken, []byte(`{}`))
			}
		}(workerID)
	}
	wg.Wait()
}
