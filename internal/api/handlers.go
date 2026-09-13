package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
	"github.com/AnkitxRot/ForgeFlow/internal/workflow"
	"github.com/google/uuid"
)

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

func writeJSONError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, errorResponse{Error: message})
}

// HealthHandler returns basic liveness status.
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ReadyHandler verifies storage connectivity.
func ReadyHandler(s store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// A lightweight query verifying store connectivity
		_, err := s.GetTenant(r.Context(), "health-check-probe")
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status": "unhealthy",
				"error":  err.Error(),
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}
}

// SubmitJobRequest defines the JSON body for submitting a job.
type SubmitJobRequest struct {
	ID                  string         `json:"id,omitempty"`
	Queue               string         `json:"queue"`
	Priority            int            `json:"priority"`
	Payload             map[string]any `json:"payload"`
	MaxRetries          int            `json:"max_retries"`
	RetryBackoffSeconds int            `json:"retry_backoff_seconds"`
	TimeoutSeconds      int            `json:"timeout_seconds"`
	IdempotencyKey      *string        `json:"idempotency_key,omitempty"`
	RunAt               *time.Time     `json:"run_at,omitempty"`
}

// CreateJobHandler handles POST /api/v1/jobs.
func CreateJobHandler(s store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := TenantFromContext(r.Context())
		if tenantID == "" {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, 1024*1024) // 1MB limit
		var req SubmitJobRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				writeJSONError(w, http.StatusRequestEntityTooLarge, "request payload exceeds 1MB limit")
				return
			}
			writeJSONError(w, http.StatusBadRequest, "invalid JSON payload")
			return
		}

		if req.Queue == "" {
			req.Queue = "default"
		}
		if req.ID == "" {
			req.ID = fmtID("job")
		}

		payloadBytes, _ := json.Marshal(req.Payload)
		if len(payloadBytes) == 0 {
			payloadBytes = []byte("{}")
		}

		now := time.Now().UTC()
		runAt := now
		if req.RunAt != nil && !req.RunAt.IsZero() {
			runAt = req.RunAt.UTC()
		}

		job := &domain.Job{
			ID:                  req.ID,
			TenantID:            tenantID,
			QueueName:           req.Queue,
			Status:              domain.StatusQueued,
			Priority:            req.Priority,
			Payload:             payloadBytes,
			MaxRetries:          req.MaxRetries,
			RetryBackoffSeconds: req.RetryBackoffSeconds,
			TimeoutSeconds:      req.TimeoutSeconds,
			RunAt:               runAt,
			IdempotencyKey:      req.IdempotencyKey,
			CreatedAt:           now,
			UpdatedAt:           now,
		}

		if err := s.CreateJob(r.Context(), job); err != nil {
			if errors.Is(err, store.ErrConflict) {
				writeJSONError(w, http.StatusConflict, "idempotency key conflict")
				return
			}
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeJSON(w, http.StatusCreated, job)
	}
}

// GetJobHandler handles GET /api/v1/jobs/{id}.
func GetJobHandler(s store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := TenantFromContext(r.Context())
		id := r.PathValue("id")
		if id == "" {
			writeJSONError(w, http.StatusBadRequest, "missing job id")
			return
		}

		job, err := s.GetJob(r.Context(), tenantID, id)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeJSONError(w, http.StatusNotFound, "job not found")
				return
			}
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeJSON(w, http.StatusOK, job)
	}
}

// CancelJobHandler handles POST /api/v1/jobs/{id}/cancel.
func CancelJobHandler(s store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := TenantFromContext(r.Context())
		id := r.PathValue("id")
		if id == "" {
			writeJSONError(w, http.StatusBadRequest, "missing job id")
			return
		}

		err := s.CancelJob(r.Context(), tenantID, id)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeJSONError(w, http.StatusNotFound, "job not found")
				return
			}
			if errors.Is(err, store.ErrTerminalState) {
				writeJSONError(w, http.StatusConflict, "cannot cancel job in terminal state")
				return
			}
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{"status": "CANCELLED"})
	}
}

// GetJobExecutionsHandler handles GET /api/v1/jobs/{id}/executions.
func GetJobExecutionsHandler(s store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := TenantFromContext(r.Context())
		id := r.PathValue("id")
		if id == "" {
			writeJSONError(w, http.StatusBadRequest, "missing job id")
			return
		}

		// First verify job belongs to tenant
		if _, err := s.GetJob(r.Context(), tenantID, id); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeJSONError(w, http.StatusNotFound, "job not found")
				return
			}
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}

		execs, err := s.GetExecutions(r.Context(), tenantID, id)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeJSON(w, http.StatusOK, execs)
	}
}

// SubmitWorkflowRequest defines JSON body for submitting a DAG workflow.
type SubmitWorkflowRequest struct {
	Definition     workflow.Definition `json:"definition"`
	IdempotencyKey *string             `json:"idempotency_key,omitempty"`
}

// CreateWorkflowHandler handles POST /api/v1/workflows.
func CreateWorkflowHandler(engine *workflow.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := TenantFromContext(r.Context())
		if tenantID == "" {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, 2*1024*1024) // 2MB limit
		var req SubmitWorkflowRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				writeJSONError(w, http.StatusRequestEntityTooLarge, "workflow definition exceeds 2MB limit")
				return
			}
			writeJSONError(w, http.StatusBadRequest, "invalid JSON payload")
			return
		}

		wf, err := engine.Submit(r.Context(), tenantID, &req.Definition, req.IdempotencyKey)
		if err != nil {
			if errors.Is(err, store.ErrConflict) {
				writeJSONError(w, http.StatusConflict, "idempotency key conflict")
				return
			}
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}

		writeJSON(w, http.StatusCreated, wf)
	}
}

// GetWorkflowHandler handles GET /api/v1/workflows/{id}.
func GetWorkflowHandler(s store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := TenantFromContext(r.Context())
		id := r.PathValue("id")
		if id == "" {
			writeJSONError(w, http.StatusBadRequest, "missing workflow id")
			return
		}

		wf, steps, err := s.GetWorkflow(r.Context(), tenantID, id)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeJSONError(w, http.StatusNotFound, "workflow not found")
				return
			}
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"workflow": wf,
			"steps":    steps,
		})
	}
}

// CancelWorkflowHandler handles POST /api/v1/workflows/{id}/cancel.
func CancelWorkflowHandler(engine *workflow.Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := TenantFromContext(r.Context())
		id := r.PathValue("id")
		if id == "" {
			writeJSONError(w, http.StatusBadRequest, "missing workflow id")
			return
		}

		if err := engine.CancelWorkflow(r.Context(), tenantID, id); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeJSONError(w, http.StatusNotFound, "workflow not found")
				return
			}
			if errors.Is(err, workflow.ErrWorkflowTerminal) {
				writeJSONError(w, http.StatusConflict, "workflow is already in a terminal state")
				return
			}
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{"status": "CANCELLED"})
	}
}

func fmtID(prefix string) string {
	return prefix + "-" + uuid.New().String()[:12]
}
