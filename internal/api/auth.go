package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"sync"
)

type contextKey string

const (
	tenantContextKey  contextKey = "tenant_id"
	requestContextKey contextKey = "request_id"
)

var (
	ErrUnauthorized = errors.New("unauthorized: valid API key required")
	ErrForbidden    = errors.New("forbidden: insufficient tenant permissions")
)

// TenantFromContext extracts the authenticated tenant ID from the request context.
func TenantFromContext(ctx context.Context) string {
	if val, ok := ctx.Value(tenantContextKey).(string); ok {
		return val
	}
	return ""
}

// WithTenant sets the tenant ID into the context.
func WithTenant(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantContextKey, tenantID)
}

// KeyValidator verifies an API key and returns the associated tenant ID.
type KeyValidator interface {
	ValidateKey(rawKey string) (tenantID string, err error)
}

// InMemoryKeyValidator stores SHA-256 hashes of API keys mapped to tenant IDs.
// Raw API keys are never stored.
type InMemoryKeyValidator struct {
	mu      sync.RWMutex
	hashToTenant map[string]string
}

// NewInMemoryKeyValidator creates an initialized InMemoryKeyValidator.
func NewInMemoryKeyValidator() *InMemoryKeyValidator {
	return &InMemoryKeyValidator{
		hashToTenant: make(map[string]string),
	}
}

// RegisterKey hashes the rawKey using SHA-256 and stores only the hash with the tenantID.
func (v *InMemoryKeyValidator) RegisterKey(rawKey, tenantID string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	hash := sha256.Sum256([]byte(rawKey))
	hashHex := hex.EncodeToString(hash[:])
	v.hashToTenant[hashHex] = tenantID
}

// ValidateKey compares the SHA-256 hash in constant time against registered hashes.
func (v *InMemoryKeyValidator) ValidateKey(rawKey string) (string, error) {
	if rawKey == "" {
		return "", ErrUnauthorized
	}

	hash := sha256.Sum256([]byte(rawKey))
	incomingHex := hex.EncodeToString(hash[:])

	v.mu.RLock()
	defer v.mu.RUnlock()

	for storedHash, tenantID := range v.hashToTenant {
		if subtle.ConstantTimeCompare([]byte(incomingHex), []byte(storedHash)) == 1 {
			return tenantID, nil
		}
	}

	return "", ErrUnauthorized
}

// AuthMiddleware creates an HTTP middleware that extracts and validates the API key.
// It supports either the `X-API-Key: <key>` header or `Authorization: Bearer <key>`.
func AuthMiddleware(validator KeyValidator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rawKey := r.Header.Get("X-API-Key")
			if rawKey == "" {
				authHeader := r.Header.Get("Authorization")
				if strings.HasPrefix(authHeader, "Bearer ") {
					rawKey = strings.TrimPrefix(authHeader, "Bearer ")
				}
			}

			if rawKey == "" {
				writeJSONError(w, http.StatusUnauthorized, "missing API key (use X-API-Key header or Authorization: Bearer)")
				return
			}

			tenantID, err := validator.ValidateKey(rawKey)
			if err != nil {
				writeJSONError(w, http.StatusUnauthorized, "invalid or revoked API key")
				return
			}

			ctx := WithTenant(r.Context(), tenantID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
