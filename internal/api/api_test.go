package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/api"
	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
	"github.com/AnkitxRot/ForgeFlow/internal/store/sqlite"
	"github.com/AnkitxRot/ForgeFlow/internal/workflow"
	"github.com/google/uuid"
)

func setupAPITestServer(t *testing.T) (http.Handler, store.Store, *api.InMemoryKeyValidator, string, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "forgeflow_api_test.db")
	s, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	tenantID := "tenant-a-" + uuid.New().String()[:8]
	err = s.CreateTenant(context.Background(), &domain.Tenant{
		ID:        tenantID,
		Name:      "Tenant Alpha",
		Status:    "ACTIVE",
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}

	queueName := "default"
	err = s.CreateQueue(context.Background(), &domain.Queue{
		TenantID:  tenantID,
		Name:      queueName,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	validator := api.NewInMemoryKeyValidator()
	apiKey := "ff_live_secret_key_alpha_12345"
	validator.RegisterKey(apiKey, tenantID)

	engine := workflow.NewEngine(s, queueName)

	router := api.NewRouter(api.ServerConfig{
		Store:        s,
		Engine:       engine,
		KeyValidator: validator,
	})

	return router, s, validator, tenantID, apiKey
}

func TestAPI_HealthAndReadiness(t *testing.T) {
	router, _, _, _, _ := setupAPITestServer(t)

	// 1. Healthz
	req := httptest.NewRequest("GET", "/healthz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for healthz, got %d", rec.Code)
	}

	// 2. Readyz
	req = httptest.NewRequest("GET", "/readyz", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for readyz, got %d", rec.Code)
	}
}

func TestAPI_AuthenticationRejection(t *testing.T) {
	router, _, _, _, _ := setupAPITestServer(t)

	// 1. Missing key
	req := httptest.NewRequest("POST", "/api/v1/jobs", bytes.NewReader([]byte(`{}`)))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing API key, got %d", rec.Code)
	}

	// 2. Invalid key
	req = httptest.NewRequest("POST", "/api/v1/jobs", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("X-API-Key", "invalid-key")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for invalid API key, got %d", rec.Code)
	}
}

func TestAPI_JobLifecycleAndTenantIsolation(t *testing.T) {
	router, s, validator, tenantA, apiKeyA := setupAPITestServer(t)

	// Register Tenant B and Key B
	tenantB := "tenant-b-" + uuid.New().String()[:8]
	_ = s.CreateTenant(context.Background(), &domain.Tenant{
		ID:        tenantB,
		Name:      "Tenant Beta",
		Status:    "ACTIVE",
		CreatedAt: time.Now().UTC(),
	})
	apiKeyB := "ff_live_secret_key_beta_67890"
	validator.RegisterKey(apiKeyB, tenantB)

	// 1. Tenant A submits a job
	body := []byte(`{"queue":"default","priority":10,"payload":{"foo":"bar"}}`)
	req := httptest.NewRequest("POST", "/api/v1/jobs", bytes.NewReader(body))
	req.Header.Set("X-API-Key", apiKeyA)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	var createdJob domain.Job
	if err := json.NewDecoder(rec.Body).Decode(&createdJob); err != nil {
		t.Fatalf("decode job response failed: %v", err)
	}
	if createdJob.TenantID != tenantA {
		t.Fatalf("expected tenant ID %s, got %s", tenantA, createdJob.TenantID)
	}

	// 2. Tenant A reads job: 200 OK
	req = httptest.NewRequest("GET", "/api/v1/jobs/"+createdJob.ID, nil)
	req.Header.Set("Authorization", "Bearer "+apiKeyA)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	// 3. Tenant B attempts to read Tenant A's job: MUST BE 404 NOT FOUND!
	req = httptest.NewRequest("GET", "/api/v1/jobs/"+createdJob.ID, nil)
	req.Header.Set("X-API-Key", apiKeyB)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("tenant isolation violation: expected 404 for cross-tenant access, got %d", rec.Code)
	}

	// 4. Tenant A cancels job: 200 OK
	req = httptest.NewRequest("POST", "/api/v1/jobs/"+createdJob.ID+"/cancel", nil)
	req.Header.Set("X-API-Key", apiKeyA)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for cancel, got %d", rec.Code)
	}

	// 5. Tenant A cancels already cancelled job: 409 Conflict
	req = httptest.NewRequest("POST", "/api/v1/jobs/"+createdJob.ID+"/cancel", nil)
	req.Header.Set("X-API-Key", apiKeyA)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict when cancelling terminal job, got %d", rec.Code)
	}
}

func TestAPI_IdempotencyKeyConflict(t *testing.T) {
	router, _, _, _, apiKey := setupAPITestServer(t)

	idempKey := "idem-order-999"
	body := []byte(`{"queue":"default","idempotency_key":"` + idempKey + `","payload":{"order_id":123}}`)

	// First submission: 201 Created
	req1 := httptest.NewRequest("POST", "/api/v1/jobs", bytes.NewReader(body))
	req1.Header.Set("X-API-Key", apiKey)
	rec1 := httptest.NewRecorder()
	router.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusCreated {
		t.Fatalf("expected 201 for first submission, got %d", rec1.Code)
	}

	// Second submission with identical idempotency key: 409 Conflict
	req2 := httptest.NewRequest("POST", "/api/v1/jobs", bytes.NewReader(body))
	req2.Header.Set("X-API-Key", apiKey)
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for duplicate idempotency key, got %d", rec2.Code)
	}
}

func TestAPI_WorkflowSubmissionAndCancellation(t *testing.T) {
	router, _, _, _, apiKey := setupAPITestServer(t)

	// 1. Submit valid workflow
	wfJSON := []byte(`{
		"definition": {
			"name": "data-pipeline",
			"steps": [
				{"name": "extract", "handler": "fetcher"},
				{"name": "transform", "handler": "cleaner", "dependencies": ["extract"]}
			]
		}
	}`)

	req := httptest.NewRequest("POST", "/api/v1/workflows", bytes.NewReader(wfJSON))
	req.Header.Set("X-API-Key", apiKey)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created for workflow, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	var createdWF domain.Workflow
	if err := json.NewDecoder(rec.Body).Decode(&createdWF); err != nil {
		t.Fatalf("decode workflow response failed: %v", err)
	}

	// 2. Get workflow status
	req = httptest.NewRequest("GET", "/api/v1/workflows/"+createdWF.ID, nil)
	req.Header.Set("X-API-Key", apiKey)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for get workflow, got %d", rec.Code)
	}

	// 3. Cancel workflow
	req = httptest.NewRequest("POST", "/api/v1/workflows/"+createdWF.ID+"/cancel", nil)
	req.Header.Set("X-API-Key", apiKey)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for workflow cancellation, got %d", rec.Code)
	}

	// 4. Submit invalid workflow with cycle: 400 Bad Request
	cyclicJSON := []byte(`{
		"definition": {
			"name": "cyclic-pipeline",
			"steps": [
				{"name": "step-a", "handler": "h1", "dependencies": ["step-b"]},
				{"name": "step-b", "handler": "h2", "dependencies": ["step-a"]}
			]
		}
	}`)

	req = httptest.NewRequest("POST", "/api/v1/workflows", bytes.NewReader(cyclicJSON))
	req.Header.Set("X-API-Key", apiKey)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for cyclic DAG, got %d", rec.Code)
	}
}

func TestArgon2id_HashingAndVerification(t *testing.T) {
	key := "ff_secret_test_key_xyz987"
	encoded, err := api.HashKey(key, api.FastArgon2Params)
	if err != nil {
		t.Fatalf("HashKey failed: %v", err)
	}

	// 1. Verify valid key matches
	match, err := api.VerifyKey(key, encoded)
	if err != nil || !match {
		t.Fatalf("expected key to verify successfully, match=%v, err=%v", match, err)
	}

	// 2. Verify invalid key fails
	match, err = api.VerifyKey("wrong-key", encoded)
	if err != nil || match {
		t.Fatalf("expected wrong key to fail verification, match=%v, err=%v", match, err)
	}

	// 3. Verify malformed hash fails cleanly
	_, err = api.VerifyKey(key, "invalid-hash-string")
	if err == nil {
		t.Fatal("expected error on malformed hash, got nil")
	}
}

func TestArgon2id_Revocation(t *testing.T) {
	validator := api.NewArgon2KeyValidator(api.FastArgon2Params)
	key := "ff_live_key_revocation_test"
	tenantID := "tenant-revocation-test"

	validator.RegisterKey(key, tenantID)

	// Validate before revocation
	gotTenant, err := validator.ValidateKey(key)
	if err != nil || gotTenant != tenantID {
		t.Fatalf("expected successful validation, got tenant=%s, err=%v", gotTenant, err)
	}

	// Revoke key
	revoked := validator.RevokeKey(key)
	if !revoked {
		t.Fatal("expected RevokeKey to return true")
	}

	// Validate after revocation must fail with ErrUnauthorized
	_, err = validator.ValidateKey(key)
	if err == nil {
		t.Fatal("expected ErrUnauthorized after revocation, got nil")
	}
}

func TestAPI_MaxPayloadSizeEnforcement(t *testing.T) {
	router, _, _, _, apiKey := setupAPITestServer(t)

	// Construct oversized payload exceeding 1MB
	hugePayload := make([]byte, 1024*1024+100)
	for i := range hugePayload {
		hugePayload[i] = 'a'
	}
	body := fmt.Sprintf(`{"queue":"default","payload":{"blob":"%s"}}`, string(hugePayload))

	req := httptest.NewRequest("POST", "/api/v1/jobs", strings.NewReader(body))
	req.Header.Set("X-API-Key", apiKey)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge && rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 413 or 400 for oversized payload, got %d", rec.Code)
	}
}
