package tests_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/retry"
	"github.com/AnkitxRot/ForgeFlow/internal/store/sqlite"
	"github.com/AnkitxRot/ForgeFlow/internal/workflow"
)

// BenchmarkValidateDAG benchmarks DAG static validation and topological sorting across 5, 20, and 100 nodes.
func BenchmarkValidateDAG(b *testing.B) {
	nodeCounts := []int{5, 20, 100}

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
