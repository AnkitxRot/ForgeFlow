package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// MaxStepOutputBytes limits step outputs to 64KB to prevent unbounded context growth.
const MaxStepOutputBytes = 64 * 1024

var (
	ErrOutputTooLarge = errors.New("step output exceeded 64KB limit")
	ErrTemplateSyntax = errors.New("malformed template expression")
	ErrMissingOutput  = errors.New("referenced step output is not available")
	ErrMissingField   = errors.New("referenced field not found in step output")
)

// regex matches {{steps.<step_name>.output.<field>}} or {{steps.<step_name>.output}}
var templateVarRegex = regexp.MustCompile(`\{\{\s*steps\.([a-zA-Z0-9_\-]+)\.output(?:\.([a-zA-Z0-9_\-\.]+))?\s*\}\}`)

// PipeContext maps step names to their completed raw JSON outputs.
type PipeContext map[string][]byte

// ResolveTemplate resolves template variables in a template byte slice using outputs from completed steps.
func ResolveTemplate(inputTemplate []byte, ctx PipeContext) ([]byte, error) {
	if len(inputTemplate) == 0 || string(inputTemplate) == "{}" {
		return []byte("{}"), nil
	}

	raw := string(inputTemplate)
	var resolveErr error

	replaced := templateVarRegex.ReplaceAllStringFunc(raw, func(match string) string {
		if resolveErr != nil {
			return match
		}

		submatches := templateVarRegex.FindStringSubmatch(match)
		if len(submatches) < 2 {
			resolveErr = fmt.Errorf("%w: %s", ErrTemplateSyntax, match)
			return match
		}

		stepName := submatches[1]
		fieldPath := ""
		if len(submatches) >= 3 {
			fieldPath = submatches[2]
		}

		outputBytes, ok := ctx[stepName]
		if !ok || len(outputBytes) == 0 {
			resolveErr = fmt.Errorf("%w for step %q", ErrMissingOutput, stepName)
			return match
		}

		if len(outputBytes) > MaxStepOutputBytes {
			resolveErr = fmt.Errorf("%w for step %q", ErrOutputTooLarge, stepName)
			return match
		}

		// If no field path specified, return whole output as string or embedded JSON
		if fieldPath == "" {
			return strings.TrimSpace(string(outputBytes))
		}

		// Traverse JSON field path
		var obj any
		if err := json.Unmarshal(outputBytes, &obj); err != nil {
			resolveErr = fmt.Errorf("failed to parse output of step %q as JSON: %w", stepName, err)
			return match
		}

		val, err := extractField(obj, fieldPath)
		if err != nil {
			resolveErr = fmt.Errorf("%w: %v in step %q", ErrMissingField, err, stepName)
			return match
		}

		valBytes, err := json.Marshal(val)
		if err != nil {
			resolveErr = err
			return match
		}

		// If string, strip outer quotes if replacing inside an existing JSON string or raw text
		valStr := string(valBytes)
		if strings.HasPrefix(valStr, "\"") && strings.HasSuffix(valStr, "\"") {
			var unquoted string
			_ = json.Unmarshal(valBytes, &unquoted)
			return unquoted
		}
		return valStr
	})

	if resolveErr != nil {
		return nil, resolveErr
	}

	return []byte(replaced), nil
}

func extractField(data any, path string) (any, error) {
	parts := strings.Split(path, ".")
	current := data

	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected map at key %q", part)
		}
		val, exists := m[part]
		if !exists {
			return nil, fmt.Errorf("key %q not found", part)
		}
		current = val
	}

	return current, nil
}
