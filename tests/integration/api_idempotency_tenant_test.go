package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AnkitxRot/ForgeFlow/internal/api"
	"github.com/AnkitxRot/ForgeFlow/internal/domain"
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
