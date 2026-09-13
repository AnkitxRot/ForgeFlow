package cli_test

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/cli"
)

func TestCLI_VersionAndHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	app := cli.NewApp(&stdout, &stderr)

	// 1. Version
	code := app.Run([]string{"version"})
	if code != 0 {
		t.Fatalf("expected exit code 0 for version, got %d", code)
	}
	if !strings.Contains(stdout.String(), "forgeflow version") {
		t.Fatalf("expected version output, got %q", stdout.String())
	}

	// 2. Help
	stdout.Reset()
	code = app.Run([]string{"help"})
	if code != 0 {
		t.Fatalf("expected exit code 0 for help, got %d", code)
	}
	if !strings.Contains(stdout.String(), "ForgeFlow - Distributed Durable Execution Platform") {
		t.Fatalf("expected help output, got %q", stdout.String())
	}

	// 3. Unknown command
	stderr.Reset()
	code = app.Run([]string{"invalid-subcommand"})
	if code != 1 {
		t.Fatalf("expected exit code 1 for invalid subcommand, got %d", code)
	}
	if !strings.Contains(stderr.String(), "unknown command") {
		t.Fatalf("expected unknown command error, got %q", stderr.String())
	}
}

func TestCLI_JobLifecycle(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "cli_job_test.db")

	var stdout, stderr bytes.Buffer
	app := cli.NewApp(&stdout, &stderr)

	// 1. Submit Job
	code := app.Run([]string{
		"job", "submit",
		"-db", dbPath,
		"-queue", "notifications",
		"-priority", "5",
		"-payload", `{"msg":"hello world"}`,
	})
	if code != 0 {
		t.Fatalf("job submit failed (code: %d, stderr: %s)", code, stderr.String())
	}
	output := stdout.String()
	if !strings.Contains(output, "Job submitted successfully") {
		t.Fatalf("expected success message, got %s", output)
	}

	// Extract job ID from output
	var jobID string
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "ID:") {
			jobID = strings.TrimSpace(strings.TrimPrefix(line, "ID:"))
			break
		}
	}
	if jobID == "" {
		t.Fatalf("failed to extract job ID from output: %s", output)
	}

	// 2. Get Job
	stdout.Reset()
	stderr.Reset()
	code = app.Run([]string{"job", "get", "-db", dbPath, "-id", jobID})
	if code != 0 {
		t.Fatalf("job get failed (code: %d, stderr: %s)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"queue_name": "notifications"`) {
		t.Fatalf("expected job details in JSON, got %s", stdout.String())
	}

	// 3. Cancel Job
	stdout.Reset()
	stderr.Reset()
	code = app.Run([]string{"job", "cancel", "-db", dbPath, "-id", jobID})
	if code != 0 {
		t.Fatalf("job cancel failed (code: %d, stderr: %s)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "cancelled successfully") {
		t.Fatalf("expected cancellation message, got %s", stdout.String())
	}
}

func TestCLI_WorkflowLifecycle(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "cli_wf_test.db")

	var stdout, stderr bytes.Buffer
	app := cli.NewApp(&stdout, &stderr)

	wfJSON := `{
		"name": "etl-pipeline",
		"steps": [
			{"name": "load", "handler": "loader"},
			{"name": "process", "handler": "processor", "dependencies": ["load"]}
		]
	}`

	// 1. Submit Workflow
	code := app.Run([]string{
		"workflow", "submit",
		"-db", dbPath,
		"-json", wfJSON,
	})
	if code != 0 {
		t.Fatalf("workflow submit failed (code: %d, stderr: %s)", code, stderr.String())
	}
	output := stdout.String()
	if !strings.Contains(output, "Workflow submitted successfully") {
		t.Fatalf("expected success message, got %s", output)
	}

	// Extract workflow ID
	var wfID string
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "ID:") {
			wfID = strings.TrimSpace(strings.TrimPrefix(line, "ID:"))
			break
		}
	}
	if wfID == "" {
		t.Fatalf("failed to extract workflow ID from output: %s", output)
	}

	// 2. Get Workflow
	stdout.Reset()
	stderr.Reset()
	code = app.Run([]string{"workflow", "get", "-db", dbPath, "-id", wfID})
	if code != 0 {
		t.Fatalf("workflow get failed (code: %d, stderr: %s)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"name": "etl-pipeline"`) {
		t.Fatalf("expected workflow details in JSON, got %s", stdout.String())
	}

	// 3. Cancel Workflow
	stdout.Reset()
	stderr.Reset()
	code = app.Run([]string{"workflow", "cancel", "-db", dbPath, "-id", wfID})
	if code != 0 {
		t.Fatalf("workflow cancel failed (code: %d, stderr: %s)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "cancelled successfully") {
		t.Fatalf("expected cancellation message, got %s", stdout.String())
	}
}

func TestCLI_WorkerLifecycle(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "cli_worker_test.db")

	var stdout, stderr bytes.Buffer
	app := cli.NewApp(&stdout, &stderr)

	// 1. Submit a job to queue
	code := app.Run([]string{
		"job", "submit",
		"-db", dbPath,
		"-queue", "worker-test-q",
		"-payload", `{"work":"test-item"}`,
	})
	if code != 0 {
		t.Fatalf("job submit failed (code: %d, stderr: %s)", code, stderr.String())
	}

	var jobID string
	for _, line := range strings.Split(stdout.String(), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "ID:") {
			jobID = strings.TrimSpace(strings.TrimPrefix(line, "ID:"))
			break
		}
	}
	if jobID == "" {
		t.Fatalf("failed to extract job ID: %s", stdout.String())
	}

	// 2. Run worker with cancellation context (auto-stops after processing)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	stdout.Reset()
	stderr.Reset()
	code = app.RunWithContext(ctx, []string{
		"worker",
		"-db", dbPath,
		"-id", "test-cli-worker",
		"-queues", "worker-test-q",
		"-concurrency", "1",
	})
	if code != 0 {
		t.Fatalf("worker failed (code: %d, stderr: %s)", code, stderr.String())
	}

	// 3. Verify the job was completed by the worker
	stdout.Reset()
	stderr.Reset()
	code = app.Run([]string{"job", "get", "-db", dbPath, "-id", jobID})
	if code != 0 {
		t.Fatalf("job get failed: %s", stderr.String())
	}
	if !strings.Contains(stdout.String(), `"status": "COMPLETED"`) {
		t.Fatalf("expected job status COMPLETED, got: %s", stdout.String())
	}
}
