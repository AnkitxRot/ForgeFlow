package telemetry

import (
	"context"
	"fmt"
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

func isSensitiveKey(key string) bool {
	lowerKey := strings.ToLower(key)
	if sensitiveKeys[lowerKey] {
		return true
	}
	substrings := []string{
		"token", "secret", "password", "api_key", "apikey", "auth", "passwd", "credential", "dsn",
	}
	for _, s := range substrings {
		if strings.Contains(lowerKey, s) {
			return true
		}
	}
	return false
}

func sanitizeLogString(val string) string {
	// Redact embedded URI passwords, e.g. postgres://user:secret@host
	if strings.Contains(val, "://") && strings.Contains(val, "@") {
		// Replace password in URI
		parts := strings.Split(val, "://")
		if len(parts) == 2 {
			subParts := strings.Split(parts[1], "@")
			if len(subParts) == 2 {
				userPass := strings.Split(subParts[0], ":")
				if len(userPass) == 2 {
					return fmt.Sprintf("%s://%s:[REDACTED]@%s", parts[0], userPass[0], subParts[1])
				}
			}
		}
	}
	return val
}

// NewRedactingHandler creates an slog Handler that redacts sensitive values.
func NewRedactingHandler(w io.Writer, level slog.Level) *RedactingHandler {
	opts := &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if isSensitiveKey(a.Key) {
				return slog.String(a.Key, "[REDACTED]")
			}
			if a.Value.Kind() == slog.KindString {
				return slog.String(a.Key, sanitizeLogString(a.Value.String()))
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
