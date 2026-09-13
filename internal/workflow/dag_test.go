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

func TestValidateDAG_Boundaries_999_1000_1001(t *testing.T) {
	makeSteps := func(count int) []workflow.StepDefinition {
		steps := make([]workflow.StepDefinition, count)
		for i := 0; i < count; i++ {
			steps[i] = workflow.StepDefinition{
				Name:    "step-" + string(rune('a'+(i%26))) + "-" + string(rune('0'+(i/26))),
				Handler: "noop",
			}
		}
		// Ensure all names are unique
		for i := 0; i < count; i++ {
			steps[i].Name = "step-" + string(rune('A'+(i%26))) + "-" + string(rune('0'+(i/26)%10)) + "-" + string(rune('0'+(i/260)%10)) + "-" + string(rune('0'+(i/2600)%10))
		}
		return steps
	}

	// 999 Steps: Valid
	def999 := &workflow.Definition{
		Name:  "dag-999",
		Steps: makeSteps(999),
	}
	order999, err := workflow.ValidateDAG(def999)
	if err != nil || len(order999) != 999 {
		t.Fatalf("expected 999 steps to pass validation, got len=%d, err=%v", len(order999), err)
	}

	// 1000 Steps: Valid (Exact limit boundary)
	def1000 := &workflow.Definition{
		Name:  "dag-1000",
		Steps: makeSteps(1000),
	}
	order1000, err := workflow.ValidateDAG(def1000)
	if err != nil || len(order1000) != 1000 {
		t.Fatalf("expected 1000 steps to pass validation, got len=%d, err=%v", len(order1000), err)
	}

	// 1001 Steps: Rejection
	def1001 := &workflow.Definition{
		Name:  "dag-1001",
		Steps: makeSteps(1001),
	}
	_, err = workflow.ValidateDAG(def1001)
	if !errors.Is(err, workflow.ErrDAGTooLarge) {
		t.Fatalf("expected ErrDAGTooLarge for 1001 steps, got: %v", err)
	}
}
