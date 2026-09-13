package api

import (
	"net/http"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/store"
	"github.com/AnkitxRot/ForgeFlow/internal/telemetry"
	"github.com/AnkitxRot/ForgeFlow/internal/workflow"
)

// ServerConfig provides dependencies and configuration for the ForgeFlow API server.
type ServerConfig struct {
	Addr         string
	Store        store.Store
	Engine       *workflow.Engine
	KeyValidator KeyValidator
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// NewRouter constructs the http.Handler with all public and authenticated endpoints.
func NewRouter(cfg ServerConfig) http.Handler {
	mux := http.NewServeMux()

	// Public Health Probes & Metrics
	mux.HandleFunc("GET /healthz", HealthHandler)
	mux.HandleFunc("GET /readyz", ReadyHandler(cfg.Store))
	mux.HandleFunc("GET /metrics", telemetry.DefaultRegistry.Handler())

	// Protected API Subtree
	apiMux := http.NewServeMux()

	// Job Endpoints
	apiMux.HandleFunc("POST /jobs", CreateJobHandler(cfg.Store))
	apiMux.HandleFunc("GET /jobs/{id}", GetJobHandler(cfg.Store))
	apiMux.HandleFunc("POST /jobs/{id}/cancel", CancelJobHandler(cfg.Store))
	apiMux.HandleFunc("GET /jobs/{id}/executions", GetJobExecutionsHandler(cfg.Store))

	// Workflow Endpoints
	if cfg.Engine != nil {
		apiMux.HandleFunc("POST /workflows", CreateWorkflowHandler(cfg.Engine))
		apiMux.HandleFunc("GET /workflows/{id}", GetWorkflowHandler(cfg.Store))
		apiMux.HandleFunc("POST /workflows/{id}/cancel", CancelWorkflowHandler(cfg.Engine))
	}

	// Mount protected routes behind AuthMiddleware
	var authWrapper func(http.Handler) http.Handler
	if cfg.KeyValidator != nil {
		authWrapper = AuthMiddleware(cfg.KeyValidator)
	} else {
		// Fallback: pass-through if no validator configured (e.g. dev)
		authWrapper = func(next http.Handler) http.Handler { return next }
	}

	mux.Handle("/api/v1/", http.StripPrefix("/api/v1", authWrapper(apiMux)))

	return mux
}

// NewServer returns a configured *http.Server.
func NewServer(cfg ServerConfig) *http.Server {
	if cfg.Addr == "" {
		cfg.Addr = ":8080"
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 10 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 30 * time.Second
	}

	return &http.Server{
		Addr:         cfg.Addr,
		Handler:      NewRouter(cfg),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}
}
