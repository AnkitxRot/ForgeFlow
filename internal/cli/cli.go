package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/api"
	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
	"github.com/AnkitxRot/ForgeFlow/internal/store/postgres"
	"github.com/AnkitxRot/ForgeFlow/internal/store/sqlite"
	"github.com/AnkitxRot/ForgeFlow/internal/version"
	"github.com/AnkitxRot/ForgeFlow/internal/worker"
	"github.com/AnkitxRot/ForgeFlow/internal/workflow"
	"github.com/google/uuid"
)

// App manages CLI command dispatch and I/O streams.
type App struct {
	Stdout io.Writer
	Stderr io.Writer
}

// NewApp creates an initialized App.
func NewApp(stdout, stderr io.Writer) *App {
	return &App{
		Stdout: stdout,
		Stderr: stderr,
	}
}

// Run executes the CLI command hierarchy based on passed arguments.
func (a *App) Run(args []string) int {
	if len(args) == 0 {
		a.printUsage()
		return 0
	}

	subcommand := args[0]
	subArgs := args[1:]

	switch subcommand {
	case "version":
		return a.runVersion()
	case "server":
		return a.runServer(subArgs)
	case "worker":
		return a.runWorker(subArgs)
	case "job":
		return a.runJob(subArgs)
	case "workflow":
		return a.runWorkflow(subArgs)
	case "help", "-h", "--help":
		a.printUsage()
		return 0
	default:
		fmt.Fprintf(a.Stderr, "unknown command: %q. Run 'forgeflow help' for usage.\n", subcommand)
		return 1
	}
}

func (a *App) printUsage() {
	fmt.Fprintln(a.Stdout, `ForgeFlow - Distributed Durable Execution Platform

Usage:
  forgeflow <command> [arguments]

Available Commands:
  server              Start the ForgeFlow API server
  worker              Start a ForgeFlow worker daemon
  job submit          Submit a new asynchronous job
  job get             Retrieve status and details of a job
  job cancel          Cancel an active or queued job
  workflow submit     Submit a declarative DAG workflow
  workflow get        Retrieve workflow state and step execution history
  workflow cancel     Cancel an active workflow DAG
  version             Display version and build information

Flags:
  Run 'forgeflow <command> -h' for command-specific flags.`)
}

func (a *App) runVersion() int {
	fmt.Fprintf(a.Stdout, "forgeflow version %s (commit: %s, built: %s)\n",
		version.Version, version.Commit, version.BuildDate)
	return 0
}

func openStore(dbType, dsn string) (store.Store, error) {
	switch strings.ToLower(dbType) {
	case "postgres", "postgresql":
		return postgres.Open(context.Background(), dsn)
	case "sqlite":
		fallthrough
	default:
		if dsn == "" {
			dsn = "forgeflow.db"
		}
		return sqlite.Open(dsn)
	}
}

func (a *App) runServer(args []string) int {
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)

	addr := fs.String("addr", ":8080", "Server listen address")
	dbType := fs.String("db-type", "sqlite", "Storage backend (sqlite or postgres)")
	dsn := fs.String("db", "forgeflow.db", "Database connection string or file path")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	s, err := openStore(*dbType, *dsn)
	if err != nil {
		fmt.Fprintf(a.Stderr, "failed to initialize store: %v\n", err)
		return 1
	}
	defer s.Close()

	wfEngine := workflow.NewEngine(s, "default")
	keyValidator := api.NewInMemoryKeyValidator()

	srv := api.NewServer(api.ServerConfig{
		Addr:         *addr,
		Store:        s,
		Engine:       wfEngine,
		KeyValidator: keyValidator,
	})

	fmt.Fprintf(a.Stdout, "Starting ForgeFlow API server on %s (backend: %s)...\n", *addr, *dbType)
	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintf(a.Stderr, "server stopped: %v\n", err)
		return 1
	}
	return 0
}

func (a *App) runWorker(args []string) int {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)

	id := fs.String("id", "", "Worker identifier (auto-generated if empty)")
	tenantID := fs.String("tenant", "default", "Tenant ID boundary")
	queuesFlag := fs.String("queues", "default", "Comma-separated queues to claim from")
	concurrency := fs.Int("concurrency", 5, "Number of concurrent job execution slots")
	dbType := fs.String("db-type", "sqlite", "Storage backend (sqlite or postgres)")
	dsn := fs.String("db", "forgeflow.db", "Database connection string or file path")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	s, err := openStore(*dbType, *dsn)
	if err != nil {
		fmt.Fprintf(a.Stderr, "failed to initialize store: %v\n", err)
		return 1
	}
	defer s.Close()

	queues := strings.Split(*queuesFlag, ",")
	for i := range queues {
		queues[i] = strings.TrimSpace(queues[i])
	}

	registry := worker.NewHandlerRegistry()
	registry.SetDefault(worker.HandlerFunc(func(ctx context.Context, job *domain.Job) ([]byte, error) {
		fmt.Fprintf(a.Stdout, "Executing job %s (queue: %s, attempt: %d)\n", job.ID, job.QueueName, job.Attempt)
		return []byte(`{"status":"completed"}`), nil
	}))

	w, err := worker.NewWorker(worker.Config{
		ID:           *id,
		TenantID:     *tenantID,
		Queues:       queues,
		Concurrency:  *concurrency,
		PollInterval: 250 * time.Millisecond,
		Store:        s,
		Registry:     registry,
	})
	if err != nil {
		fmt.Fprintf(a.Stderr, "failed to configure worker: %v\n", err)
		return 1
	}

	fmt.Fprintf(a.Stdout, "Starting worker %s (tenant: %s, queues: %v, slots: %d)...\n",
		*id, *tenantID, queues, *concurrency)

	if err := w.Start(context.Background()); err != nil {
		fmt.Fprintf(a.Stderr, "worker failed to start: %v\n", err)
		return 1
	}
	return 0
}

func (a *App) runJob(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(a.Stderr, "usage: forgeflow job <submit|get|cancel> [flags]")
		return 1
	}

	action := args[0]
	subArgs := args[1:]

	switch action {
	case "submit":
		return a.runJobSubmit(subArgs)
	case "get":
		return a.runJobGet(subArgs)
	case "cancel":
		return a.runJobCancel(subArgs)
	default:
		fmt.Fprintf(a.Stderr, "unknown job action %q. Expected: submit, get, cancel\n", action)
		return 1
	}
}

func (a *App) runJobSubmit(args []string) int {
	fs := flag.NewFlagSet("job submit", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)

	tenantID := fs.String("tenant", "default", "Tenant boundary")
	queue := fs.String("queue", "default", "Target queue name")
	priority := fs.Int("priority", 0, "Job priority")
	payloadStr := fs.String("payload", "{}", "JSON payload for the job")
	maxRetries := fs.Int("max-retries", 3, "Maximum retry attempts")
	dbType := fs.String("db-type", "sqlite", "Storage backend")
	dsn := fs.String("db", "forgeflow.db", "Database connection string or file path")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	s, err := openStore(*dbType, *dsn)
	if err != nil {
		fmt.Fprintf(a.Stderr, "failed to open store: %v\n", err)
		return 1
	}
	defer s.Close()

	ctx := context.Background()
	// Ensure tenant and queue exist
	_ = s.CreateTenant(ctx, &domain.Tenant{ID: *tenantID, Name: *tenantID, Status: "ACTIVE", CreatedAt: time.Now().UTC()})
	_ = s.CreateQueue(ctx, &domain.Queue{TenantID: *tenantID, Name: *queue, CreatedAt: time.Now().UTC()})

	now := time.Now().UTC()
	jobID := fmt.Sprintf("job-%s", uuid.New().String()[:12])
	job := &domain.Job{
		ID:                  jobID,
		TenantID:            *tenantID,
		QueueName:           *queue,
		Status:              domain.StatusQueued,
		Priority:            *priority,
		Payload:             []byte(*payloadStr),
		MaxRetries:          *maxRetries,
		RetryBackoffSeconds: 5,
		TimeoutSeconds:      300,
		RunAt:               now,
		CreatedAt:           now,
		UpdatedAt:           now,
	}

	if err := s.CreateJob(ctx, job); err != nil {
		fmt.Fprintf(a.Stderr, "failed to submit job: %v\n", err)
		return 1
	}

	fmt.Fprintf(a.Stdout, "Job submitted successfully:\n  ID:       %s\n  Queue:    %s\n  Status:   %s\n",
		job.ID, job.QueueName, job.Status)
	return 0
}

func (a *App) runJobGet(args []string) int {
	fs := flag.NewFlagSet("job get", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)

	tenantID := fs.String("tenant", "default", "Tenant boundary")
	id := fs.String("id", "", "Job ID")
	dbType := fs.String("db-type", "sqlite", "Storage backend")
	dsn := fs.String("db", "forgeflow.db", "Database connection string or file path")

	if err := fs.Parse(args); err != nil || *id == "" {
		fmt.Fprintln(a.Stderr, "usage: forgeflow job get -id <job-id> [-tenant <id>]")
		return 1
	}

	s, err := openStore(*dbType, *dsn)
	if err != nil {
		fmt.Fprintf(a.Stderr, "failed to open store: %v\n", err)
		return 1
	}
	defer s.Close()

	job, err := s.GetJob(context.Background(), *tenantID, *id)
	if err != nil {
		fmt.Fprintf(a.Stderr, "job lookup failed: %v\n", err)
		return 1
	}

	out, _ := json.MarshalIndent(job, "", "  ")
	fmt.Fprintln(a.Stdout, string(out))
	return 0
}

func (a *App) runJobCancel(args []string) int {
	fs := flag.NewFlagSet("job cancel", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)

	tenantID := fs.String("tenant", "default", "Tenant boundary")
	id := fs.String("id", "", "Job ID")
	dbType := fs.String("db-type", "sqlite", "Storage backend")
	dsn := fs.String("db", "forgeflow.db", "Database connection string or file path")

	if err := fs.Parse(args); err != nil || *id == "" {
		fmt.Fprintln(a.Stderr, "usage: forgeflow job cancel -id <job-id> [-tenant <id>]")
		return 1
	}

	s, err := openStore(*dbType, *dsn)
	if err != nil {
		fmt.Fprintf(a.Stderr, "failed to open store: %v\n", err)
		return 1
	}
	defer s.Close()

	if err := s.CancelJob(context.Background(), *tenantID, *id); err != nil {
		fmt.Fprintf(a.Stderr, "cancel failed: %v\n", err)
		return 1
	}

	fmt.Fprintf(a.Stdout, "Job %s cancelled successfully.\n", *id)
	return 0
}

func (a *App) runWorkflow(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(a.Stderr, "usage: forgeflow workflow <submit|get|cancel> [flags]")
		return 1
	}

	action := args[0]
	subArgs := args[1:]

	switch action {
	case "submit":
		return a.runWorkflowSubmit(subArgs)
	case "get":
		return a.runWorkflowGet(subArgs)
	case "cancel":
		return a.runWorkflowCancel(subArgs)
	default:
		fmt.Fprintf(a.Stderr, "unknown workflow action %q. Expected: submit, get, cancel\n", action)
		return 1
	}
}

func (a *App) runWorkflowSubmit(args []string) int {
	fs := flag.NewFlagSet("workflow submit", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)

	tenantID := fs.String("tenant", "default", "Tenant boundary")
	filePath := fs.String("file", "", "Path to DAG definition JSON file")
	jsonStr := fs.String("json", "", "Raw DAG definition JSON string")
	dbType := fs.String("db-type", "sqlite", "Storage backend")
	dsn := fs.String("db", "forgeflow.db", "Database connection string or file path")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	var data []byte
	var err error
	if *filePath != "" {
		data, err = os.ReadFile(*filePath)
		if err != nil {
			fmt.Fprintf(a.Stderr, "failed to read file %s: %v\n", *filePath, err)
			return 1
		}
	} else if *jsonStr != "" {
		data = []byte(*jsonStr)
	} else {
		fmt.Fprintln(a.Stderr, "must provide either -file or -json with workflow definition")
		return 1
	}

	var def workflow.Definition
	if err := json.Unmarshal(data, &def); err != nil {
		fmt.Fprintf(a.Stderr, "failed to parse workflow JSON: %v\n", err)
		return 1
	}

	s, err := openStore(*dbType, *dsn)
	if err != nil {
		fmt.Fprintf(a.Stderr, "failed to open store: %v\n", err)
		return 1
	}
	defer s.Close()

	ctx := context.Background()
	_ = s.CreateTenant(ctx, &domain.Tenant{ID: *tenantID, Name: *tenantID, Status: "ACTIVE", CreatedAt: time.Now().UTC()})
	_ = s.CreateQueue(ctx, &domain.Queue{TenantID: *tenantID, Name: "default", CreatedAt: time.Now().UTC()})

	engine := workflow.NewEngine(s, "default")
	wf, err := engine.Submit(ctx, *tenantID, &def, nil)
	if err != nil {
		fmt.Fprintf(a.Stderr, "workflow submit failed: %v\n", err)
		return 1
	}

	fmt.Fprintf(a.Stdout, "Workflow submitted successfully:\n  ID:     %s\n  Name:   %s\n  Status: %s\n",
		wf.ID, wf.Name, wf.Status)
	return 0
}

func (a *App) runWorkflowGet(args []string) int {
	fs := flag.NewFlagSet("workflow get", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)

	tenantID := fs.String("tenant", "default", "Tenant boundary")
	id := fs.String("id", "", "Workflow ID")
	dbType := fs.String("db-type", "sqlite", "Storage backend")
	dsn := fs.String("db", "forgeflow.db", "Database connection string or file path")

	if err := fs.Parse(args); err != nil || *id == "" {
		fmt.Fprintln(a.Stderr, "usage: forgeflow workflow get -id <workflow-id> [-tenant <id>]")
		return 1
	}

	s, err := openStore(*dbType, *dsn)
	if err != nil {
		fmt.Fprintf(a.Stderr, "failed to open store: %v\n", err)
		return 1
	}
	defer s.Close()

	wf, steps, err := s.GetWorkflow(context.Background(), *tenantID, *id)
	if err != nil {
		fmt.Fprintf(a.Stderr, "workflow lookup failed: %v\n", err)
		return 1
	}

	payload := map[string]any{
		"workflow": wf,
		"steps":    steps,
	}
	out, _ := json.MarshalIndent(payload, "", "  ")
	fmt.Fprintln(a.Stdout, string(out))
	return 0
}

func (a *App) runWorkflowCancel(args []string) int {
	fs := flag.NewFlagSet("workflow cancel", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)

	tenantID := fs.String("tenant", "default", "Tenant boundary")
	id := fs.String("id", "", "Workflow ID")
	dbType := fs.String("db-type", "sqlite", "Storage backend")
	dsn := fs.String("db", "forgeflow.db", "Database connection string or file path")

	if err := fs.Parse(args); err != nil || *id == "" {
		fmt.Fprintln(a.Stderr, "usage: forgeflow workflow cancel -id <workflow-id> [-tenant <id>]")
		return 1
	}

	s, err := openStore(*dbType, *dsn)
	if err != nil {
		fmt.Fprintf(a.Stderr, "failed to open store: %v\n", err)
		return 1
	}
	defer s.Close()

	engine := workflow.NewEngine(s, "default")
	if err := engine.CancelWorkflow(context.Background(), *tenantID, *id); err != nil {
		fmt.Fprintf(a.Stderr, "cancel failed: %v\n", err)
		return 1
	}

	fmt.Fprintf(a.Stdout, "Workflow %s cancelled successfully.\n", *id)
	return 0
}
