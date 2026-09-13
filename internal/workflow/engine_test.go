package workflow_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
	"github.com/AnkitxRot/ForgeFlow/internal/store/sqlite"
	"github.com/AnkitxRot/ForgeFlow/internal/workflow"
	"github.com/google/uuid"
)

func setupWorkflowStore(t *testing.T) (store.Store, string, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "forgeflow_wf_test.db")
	s, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	tenantID := "tenant-" + uuid.New().String()[:8]
	err = s.CreateTenant(context.Background(), &domain.Tenant{
		ID:        tenantID,
		Name:      "Workflow Test Tenant",
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

	return s, tenantID, queueName
}

func TestEngine_DiamondWorkflowLifecycleAndPiping(t *testing.T) {
	s, tenantID, queueName := setupWorkflowStore(t)
	ctx := context.Background()

	engine := workflow.NewEngine(s, queueName)

	// Diamond DAG:
	//       fetch_user
	//      /          \
	//  calc_score    notify_slack
	//      \          /
	//       finalize
	def := &workflow.Definition{
		Name: "user-onboarding",
		Steps: []workflow.StepDefinition{
			{
				Name:    "fetch_user",
				Handler: "user_service",
			},
			{
				Name:         "calc_score",
				Handler:      "score_service",
				Dependencies: []string{"fetch_user"},
				InputTemplate: map[string]any{
					"user_id": "{{steps.fetch_user.output.id}}",
				},
			},
			{
				Name:         "notify_slack",
				Handler:      "slack_service",
				Dependencies: []string{"fetch_user"},
				InputTemplate: map[string]any{
					"email": "{{steps.fetch_user.output.email}}",
				},
			},
			{
				Name:         "finalize",
				Handler:      "final_service",
				Dependencies: []string{"calc_score", "notify_slack"},
			},
		},
	}

	// 1. Submit workflow
	wf, err := engine.Submit(ctx, tenantID, def, nil)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	if wf.Status != domain.WorkflowStatusRunning {
		t.Fatalf("expected status RUNNING, got %s", wf.Status)
	}

	// Verify only root step (fetch_user) was enqueued as a job
	fetchedWF, steps, err := s.GetWorkflow(ctx, tenantID, wf.ID)
	if err != nil {
		t.Fatalf("get workflow failed: %v", err)
	}
	if fetchedWF.Status != domain.WorkflowStatusRunning {
		t.Fatalf("expected RUNNING, got %s", fetchedWF.Status)
	}

	stepMap := make(map[string]*domain.WorkflowStep)
	for _, st := range steps {
		stepMap[st.StepName] = st
	}

	if stepMap["fetch_user"].Status != domain.StatusQueued {
		t.Fatalf("fetch_user should be QUEUED, got %s", stepMap["fetch_user"].Status)
	}
	if stepMap["calc_score"].Status != domain.StatusPending {
		t.Fatalf("calc_score should be PENDING, got %s", stepMap["calc_score"].Status)
	}
	if stepMap["notify_slack"].Status != domain.StatusPending {
		t.Fatalf("notify_slack should be PENDING, got %s", stepMap["notify_slack"].Status)
	}

	// 2. Complete fetch_user with output
	err = engine.HandleStepCompletion(ctx, tenantID, wf.ID, stepMap["fetch_user"].ID, []byte(`{"id":101,"email":"alice@example.com"}`))
	if err != nil {
		t.Fatalf("handle step completion failed: %v", err)
	}

	// Both parallel branches (calc_score and notify_slack) should now be QUEUED
	_, steps, _ = s.GetWorkflow(ctx, tenantID, wf.ID)
	for _, st := range steps {
		stepMap[st.StepName] = st
	}
	if stepMap["calc_score"].Status != domain.StatusQueued {
		t.Fatalf("calc_score should now be QUEUED, got %s", stepMap["calc_score"].Status)
	}
	if stepMap["notify_slack"].Status != domain.StatusQueued {
		t.Fatalf("notify_slack should now be QUEUED, got %s", stepMap["notify_slack"].Status)
	}
	if stepMap["finalize"].Status != domain.StatusPending {
		t.Fatalf("finalize should still be PENDING, got %s", stepMap["finalize"].Status)
	}

	// 3. Complete branch 1 (calc_score)
	err = engine.HandleStepCompletion(ctx, tenantID, wf.ID, stepMap["calc_score"].ID, []byte(`{"score":98}`))
	if err != nil {
		t.Fatalf("calc_score completion failed: %v", err)
	}

	// finalize must still be PENDING because notify_slack has not finished
	_, steps, _ = s.GetWorkflow(ctx, tenantID, wf.ID)
	for _, st := range steps {
		stepMap[st.StepName] = st
	}
	if stepMap["finalize"].Status != domain.StatusPending {
		t.Fatalf("finalize must remain PENDING until all deps complete, got %s", stepMap["finalize"].Status)
	}

	// 4. Complete branch 2 (notify_slack)
	err = engine.HandleStepCompletion(ctx, tenantID, wf.ID, stepMap["notify_slack"].ID, []byte(`{"sent":true}`))
	if err != nil {
		t.Fatalf("notify_slack completion failed: %v", err)
	}

	// finalize should now be QUEUED
	_, steps, _ = s.GetWorkflow(ctx, tenantID, wf.ID)
	for _, st := range steps {
		stepMap[st.StepName] = st
	}
	if stepMap["finalize"].Status != domain.StatusQueued {
		t.Fatalf("finalize should now be QUEUED, got %s", stepMap["finalize"].Status)
	}

	// 5. Complete finalize -> Workflow becomes COMPLETED
	err = engine.HandleStepCompletion(ctx, tenantID, wf.ID, stepMap["finalize"].ID, []byte(`{"status":"all_done"}`))
	if err != nil {
		t.Fatalf("finalize completion failed: %v", err)
	}

	finalWF, _, err := s.GetWorkflow(ctx, tenantID, wf.ID)
	if err != nil {
		t.Fatalf("get workflow failed: %v", err)
	}
	if finalWF.Status != domain.WorkflowStatusCompleted {
		t.Fatalf("expected workflow status COMPLETED, got %s", finalWF.Status)
	}
	if finalWF.CompletedAt == nil {
		t.Fatal("expected workflow completed_at to be set")
	}
}

func TestEngine_StepFailure_FailsWorkflow(t *testing.T) {
	s, tenantID, queueName := setupWorkflowStore(t)
	ctx := context.Background()

	engine := workflow.NewEngine(s, queueName)

	def := &workflow.Definition{
		Name: "failing-workflow",
		Steps: []workflow.StepDefinition{
			{Name: "step-1", Handler: "task-1", FailurePolicy: domain.FailurePolicyFailWorkflow},
			{Name: "step-2", Handler: "task-2", Dependencies: []string{"step-1"}},
		},
	}

	wf, err := engine.Submit(ctx, tenantID, def, nil)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	_, steps, _ := s.GetWorkflow(ctx, tenantID, wf.ID)
	step1 := steps[0]
	step2 := steps[1]

	// Fail step 1
	err = engine.HandleStepFailure(ctx, tenantID, wf.ID, step1.ID, "database unrecoverable error")
	if err != nil {
		t.Fatalf("handle step failure failed: %v", err)
	}

	finalWF, finalSteps, _ := s.GetWorkflow(ctx, tenantID, wf.ID)
	if finalWF.Status != domain.WorkflowStatusFailed {
		t.Fatalf("expected workflow status FAILED, got %s", finalWF.Status)
	}

	stepMap := make(map[string]*domain.WorkflowStep)
	for _, st := range finalSteps {
		stepMap[st.StepName] = st
	}
	if stepMap[step1.StepName].Status != domain.StatusFailed {
		t.Fatalf("step1 should be FAILED, got %s", stepMap[step1.StepName].Status)
	}
	if stepMap[step2.StepName].Status != domain.StatusCancelled {
		t.Fatalf("step2 should be CANCELLED, got %s", stepMap[step2.StepName].Status)
	}
}

func TestEngine_CancelWorkflow(t *testing.T) {
	s, tenantID, queueName := setupWorkflowStore(t)
	ctx := context.Background()

	engine := workflow.NewEngine(s, queueName)

	def := &workflow.Definition{
		Name: "cancel-workflow",
		Steps: []workflow.StepDefinition{
			{Name: "step-1", Handler: "task-1"},
			{Name: "step-2", Handler: "task-2", Dependencies: []string{"step-1"}},
		},
	}

	wf, _ := engine.Submit(ctx, tenantID, def, nil)

	err := engine.CancelWorkflow(ctx, tenantID, wf.ID)
	if err != nil {
		t.Fatalf("cancel workflow failed: %v", err)
	}

	finalWF, steps, _ := s.GetWorkflow(ctx, tenantID, wf.ID)
	if finalWF.Status != domain.WorkflowStatusCancelled {
		t.Fatalf("expected workflow status CANCELLED, got %s", finalWF.Status)
	}
	for _, st := range steps {
		if st.Status != domain.StatusCancelled {
			t.Fatalf("expected step %s to be CANCELLED, got %s", st.StepName, st.Status)
		}
	}
}
