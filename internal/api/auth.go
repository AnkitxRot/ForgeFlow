package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/crypto/argon2"
)

type contextKey string

const (
	tenantContextKey  contextKey = "tenant_id"
	requestContextKey contextKey = "request_id"
)

var (
	ErrUnauthorized        = errors.New("unauthorized: valid API key required")
	ErrForbidden           = errors.New("forbidden: insufficient tenant permissions")
	ErrInvalidHashFormat   = errors.New("invalid encoded argon2id hash format")
	ErrIncompatibleVersion = errors.New("incompatible argon2id version")
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

// Argon2Params defines memory, iteration, and parallelism parameters for Argon2id hashing.
type Argon2Params struct {
	Memory      uint32
	Iterations  uint32
	Parallelism uint8
	SaltLength  uint32
	KeyLength   uint32
}

// DefaultArgon2Params adheres to RFC 9106 recommended production parameters.
var DefaultArgon2Params = Argon2Params{
	Memory:      64 * 1024, // 64 MB
	Iterations:  3,
	Parallelism: 2,
	SaltLength:  16,
	KeyLength:   32,
}

// FastArgon2Params provides calibrated parameters for sub-millisecond local testing.
var FastArgon2Params = Argon2Params{
	Memory:      8 * 1024, // 8 MB
	Iterations:  1,
	Parallelism: 1,
	SaltLength:  16,
	KeyLength:   32,
}

// HashKey derives a cryptographically secure Argon2id hash with a random salt.
// Returns a standard PHC-encoded string: $argon2id$v=19$m=65536,t=3,p=2$<salt-b64>$<hash-b64>
func HashKey(rawKey string, params Argon2Params) (string, error) {
	if rawKey == "" {
		return "", errors.New("rawKey cannot be empty")
	}

	salt := make([]byte, params.SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("failed to generate random salt: %w", err)
	}

	hash := argon2.IDKey([]byte(rawKey), salt, params.Iterations, params.Memory, params.Parallelism, params.KeyLength)

	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Hash := base64.RawStdEncoding.EncodeToString(hash)

	encoded := fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, params.Memory, params.Iterations, params.Parallelism, b64Salt, b64Hash)

	return encoded, nil
}

// VerifyKey checks a raw API key against an encoded Argon2id hash string in constant time.
func VerifyKey(rawKey, encodedHash string) (bool, error) {
	parts := strings.Split(encodedHash, "$")
	// Expected format: ["", "argon2id", "v=19", "m=...,t=...,p=...", "<salt>", "<hash>"]
	if len(parts) != 6 || parts[1] != "argon2id" {
		return false, ErrInvalidHashFormat
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return false, ErrInvalidHashFormat
	}
	if version != argon2.Version {
		return false, ErrIncompatibleVersion
	}

	var memory, iterations uint32
	var parallelism uint8
	subParts := strings.Split(parts[3], ",")
	if len(subParts) != 3 {
		return false, ErrInvalidHashFormat
	}
	for _, p := range subParts {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			return false, ErrInvalidHashFormat
		}
		val, err := strconv.ParseUint(kv[1], 10, 32)
		if err != nil {
			return false, ErrInvalidHashFormat
		}
		switch kv[0] {
		case "m":
			memory = uint32(val)
		case "t":
			iterations = uint32(val)
		case "p":
			parallelism = uint8(val)
		}
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, ErrInvalidHashFormat
	}

	expectedHash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, ErrInvalidHashFormat
	}

	computedHash := argon2.IDKey([]byte(rawKey), salt, iterations, memory, parallelism, uint32(len(expectedHash)))

	if subtle.ConstantTimeCompare(computedHash, expectedHash) == 1 {
		return true, nil
	}

	return false, nil
}

// KeyRecord stores metadata and hashed verifier for an API key.
type KeyRecord struct {
	TenantID    string
	EncodedHash string
	Prefix      string
	Revoked     bool
}

// Argon2KeyValidator manages in-memory Argon2id API key verification.
type Argon2KeyValidator struct {
	mu      sync.RWMutex
	params  Argon2Params
	records []*KeyRecord
}

// NewArgon2KeyValidator initializes an Argon2KeyValidator with specified parameters.
func NewArgon2KeyValidator(params Argon2Params) *Argon2KeyValidator {
	return &Argon2KeyValidator{
		params:  params,
		records: make([]*KeyRecord, 0),
	}
}

// NewInMemoryKeyValidator provides backward compatibility, defaulting to fast parameters for dev/test responsiveness.
func NewInMemoryKeyValidator() *Argon2KeyValidator {
	return NewArgon2KeyValidator(FastArgon2Params)
}

// InMemoryKeyValidator alias for backward compatibility.
type InMemoryKeyValidator = Argon2KeyValidator

// RegisterKey hashes the rawKey using Argon2id with random salt and stores the record.
func (v *Argon2KeyValidator) RegisterKey(rawKey, tenantID string) {
	prefix := ""
	if len(rawKey) >= 8 {
		prefix = rawKey[:8]
	}
	encoded, err := HashKey(rawKey, v.params)
	if err != nil {
		panic(fmt.Sprintf("failed to hash API key: %v", err))
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	v.records = append(v.records, &KeyRecord{
		TenantID:    tenantID,
		EncodedHash: encoded,
		Prefix:      prefix,
		Revoked:     false,
	})
}

// RevokeKey marks any matching key record as revoked.
func (v *Argon2KeyValidator) RevokeKey(rawKey string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	revokedAny := false
	for _, rec := range v.records {
		if rec.Revoked {
			continue
		}
		if match, _ := VerifyKey(rawKey, rec.EncodedHash); match {
			rec.Revoked = true
			revokedAny = true
		}
	}
	return revokedAny
}

// ValidateKey compares the incoming rawKey against registered Argon2id credentials.
func (v *Argon2KeyValidator) ValidateKey(rawKey string) (string, error) {
	if rawKey == "" {
		return "", ErrUnauthorized
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	for _, rec := range v.records {
		if rec.Revoked {
			continue
		}
		match, err := VerifyKey(rawKey, rec.EncodedHash)
		if err == nil && match {
			return rec.TenantID, nil
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
