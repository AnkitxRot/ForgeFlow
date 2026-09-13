package workflow_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/AnkitxRot/ForgeFlow/internal/workflow"
)

func TestResolveTemplate_ValidPiping(t *testing.T) {
	ctx := workflow.PipeContext{
		"auth_step": []byte(`{"token":"secret-xyz-123","user_id":42}`),
	}

	tmpl := []byte(`{"auth_header":"Bearer {{steps.auth_step.output.token}}","uid":{{steps.auth_step.output.user_id}}}`)
	resolved, err := workflow.ResolveTemplate(tmpl, ctx)
	if err != nil {
		t.Fatalf("unexpected template error: %v", err)
	}

	expected := `{"auth_header":"Bearer secret-xyz-123","uid":42}`
	if string(resolved) != expected {
		t.Fatalf("expected %q, got %q", expected, string(resolved))
	}
}

func TestResolveTemplate_MissingOutput(t *testing.T) {
	ctx := workflow.PipeContext{}

	tmpl := []byte(`{"header":"{{steps.missing_step.output.data}}"}`)
	_, err := workflow.ResolveTemplate(tmpl, ctx)
	if !errors.Is(err, workflow.ErrMissingOutput) {
		t.Fatalf("expected ErrMissingOutput, got %v", err)
	}
}

func TestResolveTemplate_MissingField(t *testing.T) {
	ctx := workflow.PipeContext{
		"step1": []byte(`{"present":"val"}`),
	}

	tmpl := []byte(`{"header":"{{steps.step1.output.not_present}}"}`)
	_, err := workflow.ResolveTemplate(tmpl, ctx)
	if !errors.Is(err, workflow.ErrMissingField) {
		t.Fatalf("expected ErrMissingField, got %v", err)
	}
}

func TestResolveTemplate_SecurityMatrix(t *testing.T) {
	// 1. Empty template
	res, err := workflow.ResolveTemplate([]byte(""), workflow.PipeContext{})
	if err != nil || string(res) != "{}" {
		t.Fatalf("expected empty template to return '{}', got: %s (err=%v)", string(res), err)
	}

	// 2. Nested field traversal
	ctx := workflow.PipeContext{
		"step1": []byte(`{"level1":{"level2":{"target":"found_value"}}}`),
	}
	res, err = workflow.ResolveTemplate([]byte(`{"result":"{{steps.step1.output.level1.level2.target}}"}`), ctx)
	if err != nil || string(res) != `{"result":"found_value"}` {
		t.Fatalf("nested traversal failed: %s (err=%v)", string(res), err)
	}

	// 3. Wrong type traversal (traversing into primitive)
	ctxWrong := workflow.PipeContext{
		"step1": []byte(`{"primitive": 42}`),
	}
	_, err = workflow.ResolveTemplate([]byte(`{"val":"{{steps.step1.output.primitive.child}}"}`), ctxWrong)
	if !errors.Is(err, workflow.ErrMissingField) {
		t.Fatalf("expected ErrMissingField when traversing primitive, got: %v", err)
	}

	// 4. Output boundaries: exactly 64KB output vs 64KB + 1
	exact64KBVal := make([]byte, 64*1024-15) // payload to make total output exactly 64KB
	for i := range exact64KBVal {
		exact64KBVal[i] = 'A'
	}
	ctx64KBExact := workflow.PipeContext{
		"step1": []byte(fmt.Sprintf(`{"data":"%s"}`, string(exact64KBVal))),
	}
	if len(ctx64KBExact["step1"]) == 64*1024 {
		res, err = workflow.ResolveTemplate([]byte(`{"out":"{{steps.step1.output.data}}"}`), ctx64KBExact)
		if err != nil {
			t.Fatalf("exact 64KB output should succeed, got: %v", err)
		}
	}

	// 64KB + 1 byte output
	tooLargeVal := make([]byte, 64*1024)
	ctxTooLargeOutput := workflow.PipeContext{
		"step1": []byte(fmt.Sprintf(`{"data":"%s"}`, string(tooLargeVal))),
	}
	_, err = workflow.ResolveTemplate([]byte(`{"out":"{{steps.step1.output.data}}"}`), ctxTooLargeOutput)
	if !errors.Is(err, workflow.ErrOutputTooLarge) {
		t.Fatalf("expected ErrOutputTooLarge for >64KB output, got: %v", err)
	}

	// 5. Input template boundaries: exactly 1MB vs 1MB + 1
	exact1MBInput := make([]byte, 1024*1024)
	for i := range exact1MBInput {
		exact1MBInput[i] = ' '
	}
	copy(exact1MBInput, []byte("{}"))
	res, err = workflow.ResolveTemplate(exact1MBInput, workflow.PipeContext{})
	if err != nil {
		t.Fatalf("exact 1MB input should succeed, got: %v", err)
	}

	// 1MB + 1 byte input
	tooLargeInput := make([]byte, 1024*1024+1)
	_, err = workflow.ResolveTemplate(tooLargeInput, workflow.PipeContext{})
	if !errors.Is(err, workflow.ErrInputTemplateTooLarge) {
		t.Fatalf("expected ErrInputTemplateTooLarge for >1MB input template, got: %v", err)
	}
}
