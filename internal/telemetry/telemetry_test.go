package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AnkitxRot/ForgeFlow/internal/telemetry"
)

func TestMetrics_CountersAndGauges(t *testing.T) {
	reg := telemetry.NewMetricsRegistry()
	reg.RegisterCounter("test_jobs_total", "Total test jobs")
	reg.RegisterGauge("test_active_workers", "Active test workers")

	// Inc & Add Counters
	reg.IncCounter("test_jobs_total", map[string]string{"queue": "default", "status": "success"})
	reg.AddCounter("test_jobs_total", map[string]string{"queue": "default", "status": "success"}, 4)
	reg.IncCounter("test_jobs_total", map[string]string{"queue": "high", "status": "failed"})

	// Set & Add Gauges
	reg.SetGauge("test_active_workers", map[string]string{"host": "node-1"}, 10)
	reg.AddGauge("test_active_workers", map[string]string{"host": "node-1"}, -2)

	text := reg.RenderPrometheusText()

	if !strings.Contains(text, `# HELP test_jobs_total Total test jobs`) {
		t.Fatalf("missing HELP line for test_jobs_total:\n%s", text)
	}
	if !strings.Contains(text, `# TYPE test_jobs_total counter`) {
		t.Fatalf("missing TYPE line for test_jobs_total:\n%s", text)
	}
	if !strings.Contains(text, `test_jobs_total{queue="default",status="success"} 5`) {
		t.Fatalf("missing counter line for default success:\n%s", text)
	}
	if !strings.Contains(text, `test_jobs_total{queue="high",status="failed"} 1`) {
		t.Fatalf("missing counter line for high failed:\n%s", text)
	}
	if !strings.Contains(text, `test_active_workers{host="node-1"} 8`) {
		t.Fatalf("missing gauge line for node-1:\n%s", text)
	}
}

func TestMetrics_HTTPHandler(t *testing.T) {
	reg := telemetry.NewMetricsRegistry()
	reg.RegisterCounter("http_requests_total", "HTTP requests count")
	reg.IncCounter("http_requests_total", nil)

	handler := reg.Handler()

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("expected text/plain Content-Type, got %s", rec.Header().Get("Content-Type"))
	}
	if !strings.Contains(rec.Body.String(), "http_requests_total 1") {
		t.Fatalf("expected metric in response body, got: %s", rec.Body.String())
	}
}

func TestLogger_SensitiveKeyRedaction(t *testing.T) {
	var buf bytes.Buffer
	logger := telemetry.NewLogger(&buf, slog.LevelInfo)

	logger.InfoContext(context.Background(), "user authenticated",
		slog.String("tenant_id", "tenant-100"),
		slog.String("api_key", "secret-user-token-xyz"),
		slog.String("password", "super-secret-password"),
		slog.String("token", "bearer-token-12345"),
		slog.String("normal_param", "harmless-value"),
	)

	var logMap map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logMap); err != nil {
		t.Fatalf("failed to decode JSON log output: %v (raw: %s)", err, buf.String())
	}

	if logMap["tenant_id"] != "tenant-100" {
		t.Fatalf("expected tenant-100, got %v", logMap["tenant_id"])
	}
	if logMap["normal_param"] != "harmless-value" {
		t.Fatalf("expected harmless-value, got %v", logMap["normal_param"])
	}

	// Verify sensitive keys were redacted
	if logMap["api_key"] != "[REDACTED]" {
		t.Fatalf("api_key was not redacted: %v", logMap["api_key"])
	}
	if logMap["password"] != "[REDACTED]" {
		t.Fatalf("password was not redacted: %v", logMap["password"])
	}
	if logMap["token"] != "[REDACTED]" {
		t.Fatalf("token was not redacted: %v", logMap["token"])
	}
}
