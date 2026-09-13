package workflow

import (
	"errors"
	"fmt"
	"sort"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
)

// MaxDAGSteps defines the bounded maximum number of steps permitted in a workflow DAG.
const MaxDAGSteps = 1000

var (
	ErrEmptyWorkflowName = errors.New("workflow name must not be empty")
	ErrNoStepsDefined    = errors.New("workflow must contain at least one step")
	ErrDuplicateStepName = errors.New("duplicate step name detected")
	ErrMissingDependency = errors.New("step references non-existent dependency")
	ErrSelfDependency    = errors.New("step cannot depend on itself")
	ErrCycleDetected     = errors.New("cycle detected in workflow DAG")
	ErrEmptyStepName     = errors.New("step name must not be empty")
	ErrEmptyStepHandler  = errors.New("step handler must not be empty")
	ErrDAGTooLarge       = errors.New("workflow exceeds maximum allowed step count of 1000")
)

// Definition represents the declarative structure of a workflow DAG.
type Definition struct {
	Name        string           `json:"name"`
	Description string           `json:"description,omitempty"`
	Steps       []StepDefinition `json:"steps"`
}

// StepDefinition represents a single step node within a workflow DAG.
type StepDefinition struct {
	Name          string               `json:"name"`
	Handler       string               `json:"handler"`
	Queue         string               `json:"queue,omitempty"`
	Dependencies  []string             `json:"dependencies,omitempty"`
	InputTemplate map[string]any       `json:"input_template,omitempty"`
	FailurePolicy domain.FailurePolicy `json:"failure_policy,omitempty"`
}

// ValidateDAG performs comprehensive static validation on the DAG:
// 1. Validates workflow and step naming
// 2. Checks for duplicates
// 3. Verifies all referenced dependencies exist
// 4. Verifies no self-cycles
// 5. Executes Three-Color DFS to guarantee cycle-free (Acyclic) property
// 6. Returns deterministic topological ordering
func ValidateDAG(def *Definition) ([]StepDefinition, error) {
	if def == nil {
		return nil, errors.New("workflow definition must not be nil")
	}
	if def.Name == "" {
		return nil, ErrEmptyWorkflowName
	}
	if len(def.Steps) == 0 {
		return nil, ErrNoStepsDefined
	}
	if len(def.Steps) > MaxDAGSteps {
		return nil, ErrDAGTooLarge
	}

	stepMap := make(map[string]StepDefinition, len(def.Steps))
	for _, step := range def.Steps {
		if step.Name == "" {
			return nil, ErrEmptyStepName
		}
		if step.Handler == "" {
			return nil, fmt.Errorf("%w for step %q", ErrEmptyStepHandler, step.Name)
		}
		if _, exists := stepMap[step.Name]; exists {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateStepName, step.Name)
		}
		stepMap[step.Name] = step
	}

	// Validate dependency references
	for _, step := range def.Steps {
		for _, dep := range step.Dependencies {
			if dep == step.Name {
				return nil, fmt.Errorf("%w: step %q depends on itself", ErrSelfDependency, step.Name)
			}
			if _, exists := stepMap[dep]; !exists {
				return nil, fmt.Errorf("%w: step %q references missing dependency %q", ErrMissingDependency, step.Name, dep)
			}
		}
	}

	// Three-Color DFS Cycle Detection:
	// 0: White (unvisited)
	// 1: Gray (visiting / in recursion stack)
	// 2: Black (visited / completed)
	const (
		white = 0
		gray  = 1
		black = 2
	)

	colors := make(map[string]int, len(def.Steps))
	for _, step := range def.Steps {
		colors[step.Name] = white
	}

	var topologicalOrder []StepDefinition
	var dfs func(stepName string) error

	dfs = func(stepName string) error {
		colors[stepName] = gray

		// Sort dependencies alphabetically for deterministic traversal
		deps := append([]string(nil), stepMap[stepName].Dependencies...)
		sort.Strings(deps)

		for _, dep := range deps {
			if colors[dep] == gray {
				return fmt.Errorf("%w: cycle path involves %q -> %q", ErrCycleDetected, stepName, dep)
			}
			if colors[dep] == white {
				if err := dfs(dep); err != nil {
					return err
				}
			}
		}

		colors[stepName] = black
		topologicalOrder = append(topologicalOrder, stepMap[stepName])
		return nil
	}

	// Deterministic root traversal: sort all step names
	sortedNames := make([]string, 0, len(def.Steps))
	for name := range stepMap {
		sortedNames = append(sortedNames, name)
	}
	sort.Strings(sortedNames)

	for _, name := range sortedNames {
		if colors[name] == white {
			if err := dfs(name); err != nil {
				return nil, err
			}
		}
	}

	return topologicalOrder, nil
}
