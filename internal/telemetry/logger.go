package telemetry

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

var sensitiveKeys = map[string]bool{
	"api_key":       true,
	"secret":        true,
	"password":      true,
	"token":         true,
	"authorization": true,
	"lease_token":   true,
}

// RedactingHandler wraps an slog.Handler to sanitize sensitive field values.
type RedactingHandler struct {
	inner slog.Handler
}

// NewRedactingHandler creates an slog Handler that redacts sensitive values.
func NewRedactingHandler(w io.Writer, level slog.Level) *RedactingHandler {
	opts := &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			lowerKey := strings.ToLower(a.Key)
			if sensitiveKeys[lowerKey] {
				return slog.String(a.Key, "[REDACTED]")
			}
			return a
		},
	}
	return &RedactingHandler{
		inner: slog.NewJSONHandler(w, opts),
	}
}

func (h *RedactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *RedactingHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.inner.Handle(ctx, r)
}

func (h *RedactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &RedactingHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *RedactingHandler) WithGroup(name string) slog.Handler {
	return &RedactingHandler{inner: h.inner.WithGroup(name)}
}

// NewLogger creates an initialized slog.Logger emitting structured JSON with sensitive key redaction.
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	return slog.New(NewRedactingHandler(w, level))
}
