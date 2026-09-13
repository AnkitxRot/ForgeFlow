package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AnkitxRot/ForgeFlow/internal/api"
	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
	"github.com/AnkitxRot/ForgeFlow/internal/workflow"
)

func TestPostgres_API_Idempotency_ConcurrentStorm(t *testing.T) {
	s := getTestPostgresStore(t)
	validator := api.NewInMemoryKeyValidator()

	tenantID := "tenant-idem-api-" + uuid.NewString()[:8]
	apiKey := "ff_live_key_" + tenantID
	validator.RegisterKey(apiKey, tenantID)

	setupTenantAndQueue(t, s, tenantID, "default")
	router := api.NewRouter(api.ServerConfig{
		Store:        s,
		Engine:       workflow.NewEngine(s, "default"),
		KeyValidator: validator,
	})

	idempotencyKey := "order-checkout-" + uuid.NewString()
	numRequests := 25

	var wg sync.WaitGroup
	wg.Add(numRequests)

	var successCount atomic.Int64
	var conflictCount atomic.Int64

	startBarrier := make(chan struct{})

	for i := 0; i < numRequests; i++ {
		go func(idx int) {
			defer wg.Done()
			<-startBarrier

			reqBody, _ := json.Marshal(map[string]any{
				"queue":           "default",
				"idempotency_key": idempotencyKey,
				"payload": map[string]any{
					"order_id": 999,
					"idx":      idx,
				},
			})

			req := httptest.NewRequest("POST", "/api/v1/jobs", bytes.NewReader(reqBody))
			req.Header.Set("X-API-Key", apiKey)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code == http.StatusCreated {
				successCount.Add(1)
			} else if rec.Code == http.StatusConflict {
				conflictCount.Add(1)
			} else {
				t.Errorf("unexpected status code: %d, body: %s", rec.Code, rec.Body.String())
			}
		}(i)
	}

	close(startBarrier)
	wg.Wait()

	// Invariant: Exactly 1 request creates the job, all other 24 return 409 Conflict
	if successCount.Load() != 1 {
		t.Fatalf("expected exactly 1 successful creation, got %d", successCount.Load())
	}
	if conflictCount.Load() != int64(numRequests-1) {
		t.Fatalf("expected %d conflict responses, got %d", numRequests-1, conflictCount.Load())
	}
}

func TestPostgres_API_TenantIsolation_Attacks(t *testing.T) {
	s := getTestPostgresStore(t)
	validator := api.NewInMemoryKeyValidator()

	tenantA := "tenant-a-" + uuid.NewString()[:8]
	keyA := "key-a-" + tenantA
	validator.RegisterKey(keyA, tenantA)
	setupTenantAndQueue(t, s, tenantA, "default")

	tenantB := "tenant-b-" + uuid.NewString()[:8]
	keyB := "key-b-" + tenantB
	validator.RegisterKey(keyB, tenantB)
	setupTenantAndQueue(t, s, tenantB, "default")

	router := api.NewRouter(api.ServerConfig{
		Store:        s,
		Engine:       workflow.NewEngine(s, "default"),
		KeyValidator: validator,
	})

	ctx := context.Background()

	// 1. Tenant A creates Job A
	jobA := &domain.Job{
		ID:        "job-a-secret-" + uuid.NewString()[:8],
		TenantID:  tenantA,
		QueueName: "default",
		Status:    domain.StatusQueued,
		Payload:   []byte(`{"secret_a":"confidential"}`),
	}
	if err := s.CreateJob(ctx, jobA); err != nil {
		t.Fatalf("create job A failed: %v", err)
	}

	// 2. Tenant B attempts to read Job A -> MUST be 404 Not Found
	req := httptest.NewRequest("GET", "/api/v1/jobs/"+jobA.ID, nil)
	req.Header.Set("X-API-Key", keyB)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("SECURITY VIOLATION: Tenant B read Tenant A job: status %d, body: %s", rec.Code, rec.Body.String())
	}

	// 3. Tenant B attempts to cancel Job A -> MUST be 404 Not Found
	req = httptest.NewRequest("POST", "/api/v1/jobs/"+jobA.ID+"/cancel", nil)
	req.Header.Set("X-API-Key", keyB)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("SECURITY VIOLATION: Tenant B cancelled Tenant A job: status %d, body: %s", rec.Code, rec.Body.String())
	}

	// 4. Tenant B attempts to get execution history for Job A -> MUST be 404 Not Found
	req = httptest.NewRequest("GET", "/api/v1/jobs/"+jobA.ID+"/executions", nil)
	req.Header.Set("X-API-Key", keyB)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("SECURITY VIOLATION: Tenant B got execution history of Tenant A: status %d", rec.Code)
	}

	// 5. Tenant A creates Workflow A
	now := time.Now().UTC()
	wfA := &domain.Workflow{
		ID:             "wf-a-secret-" + uuid.NewString()[:8],
		TenantID:       tenantA,
		Name:           "Pipeline A",
		Status:         domain.WorkflowStatusRunning,
		DefinitionJSON: []byte(`{}`),
		ContextData:    []byte(`{}`),
		CreatedAt:      now,
	}
	if err := s.CreateWorkflow(ctx, wfA, nil); err != nil {
		t.Fatalf("create workflow A failed: %v", err)
	}

	// 6. Tenant B attempts to get Workflow A -> MUST be 404 Not Found
	req = httptest.NewRequest("GET", "/api/v1/workflows/"+wfA.ID, nil)
	req.Header.Set("X-API-Key", keyB)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("SECURITY VIOLATION: Tenant B read Tenant A workflow: status %d", rec.Code)
	}

	// 7. Tenant B attempts to cancel Workflow A -> MUST be 404 Not Found
	req = httptest.NewRequest("POST", "/api/v1/workflows/"+wfA.ID+"/cancel", nil)
	req.Header.Set("X-API-Key", keyB)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("SECURITY VIOLATION: Tenant B cancelled Tenant A workflow: status %d", rec.Code)
	}
}

// TestPostgres_Store_AllMethods_TenantIsolation executes Phase 9 forensics across all store operations:
// Proves that every SQL read and write mutation strictly enforces tenant isolation at the database layer.
func TestPostgres_Store_AllMethods_TenantIsolation(t *testing.T) {
	s := getTestPostgresStore(t)
	ctx := context.Background()

	tenantA := "tenant-store-a-" + uuid.NewString()[:8]
	queueA := "queue-a-" + uuid.NewString()[:8]
	setupTenantAndQueue(t, s, tenantA, queueA)

	tenantB := "tenant-store-b-" + uuid.NewString()[:8]
	queueB := "queue-b-" + uuid.NewString()[:8]
	setupTenantAndQueue(t, s, tenantB, queueB)

	// 1. Create Job A under Tenant A and claim it
	jobA := &domain.Job{
		ID:        "job-tenant-a-" + uuid.NewString()[:8],
		TenantID:  tenantA,
		QueueName: queueA,
		Status:    domain.StatusQueued,
		Payload:   []byte(`{"tenant_data":"confidential"}`),
	}
	if err := s.CreateJob(ctx, jobA); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	claimedA, err := s.ClaimJobs(ctx, tenantA, "worker-a", []string{queueA}, 1, 30*time.Second)
	if err != nil || len(claimedA) != 1 {
		t.Fatalf("claim failed: %v", err)
	}
	cA := claimedA[0]

	// 2. Tenant B attempts to GetJob A
	_, err = s.GetJob(ctx, tenantB, cA.ID)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetJob cross-tenant must return ErrNotFound, got: %v", err)
	}

	// 3. Tenant B attempts CompleteJob A
	err = s.CompleteJob(ctx, tenantB, cA.ID, cA.FencingGeneration, *cA.LeaseToken, []byte(`{"spoofed":true}`))
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("CompleteJob cross-tenant must return ErrNotFound, got: %v", err)
	}

	// 4. Tenant B attempts FailJob A
	err = s.FailJob(ctx, tenantB, cA.ID, cA.FencingGeneration, *cA.LeaseToken, "spoofed fail", false, 0)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("FailJob cross-tenant must return ErrNotFound, got: %v", err)
	}

	// 5. Tenant B attempts RenewLease A
	err = s.RenewLease(ctx, tenantB, cA.ID, cA.FencingGeneration, *cA.LeaseToken, 10*time.Second)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("RenewLease cross-tenant must return ErrNotFound, got: %v", err)
	}

	// 6. Tenant B attempts CancelJob A
	err = s.CancelJob(ctx, tenantB, cA.ID)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("CancelJob cross-tenant must return ErrNotFound, got: %v", err)
	}

	// 7. Tenant B attempts GetExecutions for Job A
	execs, err := s.GetExecutions(ctx, tenantB, cA.ID)
	if err != nil {
		t.Fatalf("GetExecutions cross-tenant returned unexpected error: %v", err)
	}
	if len(execs) != 0 {
		t.Fatalf("SECURITY FAULT: Tenant B retrieved %d executions for Tenant A's job", len(execs))
	}

	// 8. Create Workflow A under Tenant A with a Step
	wfA := &domain.Workflow{
		ID:             "wf-tenant-a-" + uuid.NewString()[:8],
		TenantID:       tenantA,
		Name:           "Workflow A",
		Status:         domain.WorkflowStatusRunning,
		DefinitionJSON: []byte(`{}`),
		ContextData:    []byte(`{}`),
		CreatedAt:      time.Now().UTC(),
	}
	stepA := &domain.WorkflowStep{
		ID:            "step-tenant-a-" + uuid.NewString()[:8],
		WorkflowID:    wfA.ID,
		StepName:      "step-1",
		Handler:       "task-handler",
		Status:        domain.StatusQueued,
		Dependencies:  []string{},
		InputTemplate: []byte("{}"),
		FailurePolicy: domain.FailurePolicyFailWorkflow,
		CreatedAt:     time.Now().UTC(),
	}
	if err := s.CreateWorkflow(ctx, wfA, []*domain.WorkflowStep{stepA}); err != nil {
		t.Fatalf("create workflow failed: %v", err)
	}

	// 9. Tenant B attempts GetWorkflow A
	_, _, err = s.GetWorkflow(ctx, tenantB, wfA.ID)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetWorkflow cross-tenant must return ErrNotFound, got: %v", err)
	}

	// 10. Tenant B attempts UpdateWorkflowStatus A
	msg := "tampered"
	err = s.UpdateWorkflowStatus(ctx, tenantB, wfA.ID, domain.WorkflowStatusFailed, &msg)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("UpdateWorkflowStatus cross-tenant must return ErrNotFound, got: %v", err)
	}

	// 11. Tenant B attempts UpdateWorkflowStep A
	err = s.UpdateWorkflowStep(ctx, tenantB, stepA.ID, domain.StatusCompleted, []byte(`{"fake":true}`), nil)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("UpdateWorkflowStep cross-tenant must return ErrNotFound, got: %v", err)
	}

	// 12. Tenant B attempts GetQueue for Queue A
	_, err = s.GetQueue(ctx, tenantB, queueA)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetQueue cross-tenant must return ErrNotFound, got: %v", err)
	}

	// 13. Tenant B attempts ClaimJobs from Queue A
	claimedCross, err := s.ClaimJobs(ctx, tenantB, "worker-b", []string{queueA}, 10, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimJobs cross-tenant claim returned error: %v", err)
	}
	if len(claimedCross) != 0 {
		t.Fatalf("SECURITY VIOLATION: Tenant B claimed %d jobs belonging to Tenant A", len(claimedCross))
	}
}
