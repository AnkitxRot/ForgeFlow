package workflow_test

import (
	"errors"
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
