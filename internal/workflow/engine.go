package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
	"github.com/AnkitxRot/ForgeFlow/internal/store"
	"github.com/google/uuid"
)

var (
	ErrWorkflowTerminal = errors.New("workflow is in a terminal state and cannot be modified")
	ErrStepNotFound     = errors.New("workflow step not found")
)

// Engine orchestrates declarative DAG execution, dependency scheduling, and output piping.
type Engine struct {
	store        store.Store
	defaultQueue string
}

// NewEngine creates an Engine instance.
func NewEngine(s store.Store, defaultQueue string) *Engine {
	if defaultQueue == "" {
		defaultQueue = "default"
	}
	return &Engine{
		store:        s,
		defaultQueue: defaultQueue,
	}
}

// Submit validates the DAG and registers the workflow and initial ready jobs.
func (e *Engine) Submit(ctx context.Context, tenantID string, def *Definition, idempotencyKey *string) (*domain.Workflow, error) {
	if tenantID == "" {
		return nil, domain.ErrEmptyTenantID
	}

	// 1. Static Validation & Topological Sort
	_, err := ValidateDAG(def)
	if err != nil {
		return nil, fmt.Errorf("DAG validation failed: %w", err)
	}

	defJSON, err := json.Marshal(def)
	if err != nil {
		return nil, err
	}

	wfID := fmt.Sprintf("wf-%s", uuid.New().String()[:12])
	now := time.Now().UTC()

	wf := &domain.Workflow{
		ID:             wfID,
		TenantID:       tenantID,
		Name:           def.Name,
		Status:         domain.WorkflowStatusRunning,
		IdempotencyKey: idempotencyKey,
		DefinitionJSON: defJSON,
		ContextData:    []byte("{}"),
		CreatedAt:      now,
		StartedAt:      &now,
	}

	steps := make([]*domain.WorkflowStep, 0, len(def.Steps))
	stepMap := make(map[string]*domain.WorkflowStep, len(def.Steps))

	for _, stepDef := range def.Steps {
		stepID := fmt.Sprintf("step-%s", uuid.New().String()[:12])
		inputBytes, _ := json.Marshal(stepDef.InputTemplate)
		if len(inputBytes) == 0 {
			inputBytes = []byte("{}")
		}
		policy := stepDef.FailurePolicy
		if policy == "" {
			policy = domain.FailurePolicyFailWorkflow
		}

		step := &domain.WorkflowStep{
			ID:            stepID,
			WorkflowID:    wfID,
			StepName:      stepDef.Name,
			Status:        domain.StatusPending,
			Dependencies:  stepDef.Dependencies,
			Handler:       stepDef.Handler,
			InputTemplate: inputBytes,
			FailurePolicy: policy,
			CreatedAt:     now,
		}
		steps = append(steps, step)
		stepMap[stepDef.Name] = step
	}

	// 2. Atomically persist workflow and all step definitions
	if err := e.store.CreateWorkflow(ctx, wf, steps); err != nil {
		return nil, err
	}

	// 3. Dispatch initial ready steps (in-degree 0) into queue
	for _, stepDef := range def.Steps {
		if len(stepDef.Dependencies) == 0 {
			step := stepMap[stepDef.Name]
			queue := stepDef.Queue
			if queue == "" {
				queue = e.defaultQueue
			}

			// Enqueue Job for step
			jobID := fmt.Sprintf("job-%s", uuid.New().String()[:12])
			payloadMap := map[string]any{
				"handler": step.Handler,
				"step":    step.StepName,
			}
			if len(step.InputTemplate) > 0 && string(step.InputTemplate) != "{}" {
				var customInput any
				if err := json.Unmarshal(step.InputTemplate, &customInput); err == nil {
					payloadMap["input"] = customInput
				}
			}
			payloadBytes, _ := json.Marshal(payloadMap)

			job := &domain.Job{
				ID:             jobID,
				TenantID:       tenantID,
				QueueName:      queue,
				WorkflowID:     &wf.ID,
				WorkflowStepID: &step.ID,
				Status:         domain.StatusQueued,
				RunAt:          now,
				Payload:        payloadBytes,
				CreatedAt:      now,
				UpdatedAt:      now,
			}
			if err := e.store.CreateJob(ctx, job); err != nil {
				return nil, err
			}

			// Mark step as QUEUED
			_ = e.store.UpdateWorkflowStep(ctx, tenantID, step.ID, domain.StatusQueued, nil, nil)
		}
	}

	return wf, nil
}

// HandleStepCompletion records step success, resolves dependent input templates, and enqueues newly ready steps.
func (e *Engine) HandleStepCompletion(ctx context.Context, tenantID, workflowID, stepID string, output []byte) error {
	if len(output) > MaxStepOutputBytes {
		return ErrOutputTooLarge
	}

	// 1. Mark step COMPLETED with output
	if err := e.store.UpdateWorkflowStep(ctx, tenantID, stepID, domain.StatusCompleted, output, nil); err != nil {
		return err
	}

	// 2. Fetch parent workflow and full step state
	wf, steps, err := e.store.GetWorkflow(ctx, tenantID, workflowID)
	if err != nil {
		return err
	}
	if wf.Status.IsTerminal() {
		return ErrWorkflowTerminal
	}

	// 3. Build step lookup & output context
	statusMap := make(map[string]domain.JobStatus, len(steps))
	pipeCtx := make(PipeContext, len(steps))
	allCompleted := true

	for _, s := range steps {
		statusMap[s.StepName] = s.Status
		if s.Status != domain.StatusCompleted {
			allCompleted = false
		}
		if len(s.OutputData) > 0 {
			pipeCtx[s.StepName] = s.OutputData
		}
	}

	// 4. If all steps completed, transition workflow to COMPLETED
	if allCompleted {
		return e.store.UpdateWorkflowStatus(ctx, tenantID, wf.ID, domain.WorkflowStatusCompleted, nil)
	}

	// 5. Evaluate remaining PENDING steps for ready dependencies
	now := time.Now().UTC()
	for _, s := range steps {
		if s.Status != domain.StatusPending {
			continue
		}

		// Check if all prerequisites are COMPLETED
		depsSatisfied := true
		for _, dep := range s.Dependencies {
			if statusMap[dep] != domain.StatusCompleted {
				depsSatisfied = false
				break
			}
		}

		if depsSatisfied {
			// Resolve template variables from upstream step outputs
			resolvedInput, err := ResolveTemplate(s.InputTemplate, pipeCtx)
			if err != nil {
				errMsg := err.Error()
				_ = e.store.UpdateWorkflowStep(ctx, tenantID, s.ID, domain.StatusFailed, nil, &errMsg)
				_ = e.store.UpdateWorkflowStatus(ctx, tenantID, wf.ID, domain.WorkflowStatusFailed, &errMsg)
				return err
			}

			// Enqueue job for ready step
			jobID := fmt.Sprintf("job-%s", uuid.New().String()[:12])
			payloadMap := map[string]any{
				"handler":     s.Handler,
				"step":        s.StepName,
				"workflow_id": wf.ID,
				"step_id":     s.ID,
			}
			if len(resolvedInput) > 0 && string(resolvedInput) != "{}" {
				var customInput any
				if err := json.Unmarshal(resolvedInput, &customInput); err == nil {
					payloadMap["input"] = customInput
				}
			}
			payloadBytes, _ := json.Marshal(payloadMap)

			job := &domain.Job{
				ID:             jobID,
				TenantID:       tenantID,
				QueueName:      e.defaultQueue,
				WorkflowID:     &wf.ID,
				WorkflowStepID: &s.ID,
				Status:         domain.StatusQueued,
				RunAt:          now,
				Payload:        payloadBytes,
				CreatedAt:      now,
				UpdatedAt:      now,
			}
			if err := e.store.CreateJob(ctx, job); err != nil {
				return err
			}

			_ = e.store.UpdateWorkflowStep(ctx, tenantID, s.ID, domain.StatusQueued, resolvedInput, nil)
		}
	}

	return nil
}

// HandleStepFailure handles a step error according to its failure policy.
func (e *Engine) HandleStepFailure(ctx context.Context, tenantID, workflowID, stepID, errMsg string) error {
	_ = e.store.UpdateWorkflowStep(ctx, tenantID, stepID, domain.StatusFailed, nil, &errMsg)

	wf, steps, err := e.store.GetWorkflow(ctx, tenantID, workflowID)
	if err != nil {
		return err
	}

	var targetStep *domain.WorkflowStep
	for _, s := range steps {
		if s.ID == stepID {
			targetStep = s
			break
		}
	}

	if targetStep != nil && targetStep.FailurePolicy == domain.FailurePolicyFailWorkflow {
		// Fail workflow and cancel remaining steps
		_ = e.store.UpdateWorkflowStatus(ctx, tenantID, wf.ID, domain.WorkflowStatusFailed, &errMsg)
		for _, s := range steps {
			if s.Status == domain.StatusPending || s.Status == domain.StatusQueued {
				_ = e.store.UpdateWorkflowStep(ctx, tenantID, s.ID, domain.StatusCancelled, nil, nil)
			}
		}
	}

	return nil
}

// CancelWorkflow transitions the workflow and all non-terminal steps to CANCELLED.
func (e *Engine) CancelWorkflow(ctx context.Context, tenantID, workflowID string) error {
	wf, steps, err := e.store.GetWorkflow(ctx, tenantID, workflowID)
	if err != nil {
		return err
	}
	if wf.Status.IsTerminal() {
		return ErrWorkflowTerminal
	}

	if err := e.store.UpdateWorkflowStatus(ctx, tenantID, workflowID, domain.WorkflowStatusCancelled, nil); err != nil {
		return err
	}

	for _, s := range steps {
		if !s.Status.IsTerminal() {
			_ = e.store.UpdateWorkflowStep(ctx, tenantID, s.ID, domain.StatusCancelled, nil, nil)
		}
	}
	return nil
}
