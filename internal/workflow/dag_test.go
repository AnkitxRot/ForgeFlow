package workflow_test

import (
	"errors"
	"testing"

	"github.com/AnkitxRot/ForgeFlow/internal/workflow"
)

func TestValidateDAG_ValidLinear(t *testing.T) {
	def := &workflow.Definition{
		Name: "linear-workflow",
		Steps: []workflow.StepDefinition{
			{Name: "step-1", Handler: "task-1"},
			{Name: "step-2", Handler: "task-2", Dependencies: []string{"step-1"}},
			{Name: "step-3", Handler: "task-3", Dependencies: []string{"step-2"}},
		},
	}

	order, err := workflow.ValidateDAG(def)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	if len(order) != 3 {
		t.Fatalf("expected 3 steps in topological order, got %d", len(order))
	}
	if order[0].Name != "step-1" || order[1].Name != "step-2" || order[2].Name != "step-3" {
		t.Fatalf("invalid topological order: %v, %v, %v", order[0].Name, order[1].Name, order[2].Name)
	}
}

func TestValidateDAG_ValidDiamondParallel(t *testing.T) {
	def := &workflow.Definition{
		Name: "diamond-workflow",
		Steps: []workflow.StepDefinition{
			{Name: "start", Handler: "init"},
			{Name: "branch-a", Handler: "proc-a", Dependencies: []string{"start"}},
			{Name: "branch-b", Handler: "proc-b", Dependencies: []string{"start"}},
			{Name: "join", Handler: "merge", Dependencies: []string{"branch-a", "branch-b"}},
		},
	}

	order, err := workflow.ValidateDAG(def)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if len(order) != 4 {
		t.Fatalf("expected 4 steps, got %d", len(order))
	}
	if order[0].Name != "start" || order[3].Name != "join" {
		t.Fatalf("start must be first and join must be last in topological sort: %v", order)
	}
}

func TestValidateDAG_DuplicateStepName(t *testing.T) {
	def := &workflow.Definition{
		Name: "duplicate-steps",
		Steps: []workflow.StepDefinition{
			{Name: "step-1", Handler: "task-1"},
			{Name: "step-1", Handler: "task-2"},
		},
	}

	_, err := workflow.ValidateDAG(def)
	if !errors.Is(err, workflow.ErrDuplicateStepName) {
		t.Fatalf("expected ErrDuplicateStepName, got %v", err)
	}
}

func TestValidateDAG_MissingDependency(t *testing.T) {
	def := &workflow.Definition{
		Name: "missing-dep",
		Steps: []workflow.StepDefinition{
			{Name: "step-1", Handler: "task-1", Dependencies: []string{"non-existent"}},
		},
	}

	_, err := workflow.ValidateDAG(def)
	if !errors.Is(err, workflow.ErrMissingDependency) {
		t.Fatalf("expected ErrMissingDependency, got %v", err)
	}
}

func TestValidateDAG_SelfCycle(t *testing.T) {
	def := &workflow.Definition{
		Name: "self-cycle",
		Steps: []workflow.StepDefinition{
			{Name: "step-1", Handler: "task-1", Dependencies: []string{"step-1"}},
		},
	}

	_, err := workflow.ValidateDAG(def)
	if !errors.Is(err, workflow.ErrSelfDependency) {
		t.Fatalf("expected ErrSelfDependency, got %v", err)
	}
}

func TestValidateDAG_MultiNodeCycle(t *testing.T) {
	def := &workflow.Definition{
		Name: "multi-node-cycle",
		Steps: []workflow.StepDefinition{
			{Name: "step-a", Handler: "task-a", Dependencies: []string{"step-c"}},
			{Name: "step-b", Handler: "task-b", Dependencies: []string{"step-a"}},
			{Name: "step-c", Handler: "task-c", Dependencies: []string{"step-b"}},
		},
	}

	_, err := workflow.ValidateDAG(def)
	if !errors.Is(err, workflow.ErrCycleDetected) {
		t.Fatalf("expected ErrCycleDetected, got %v", err)
	}
}
