package retry_test

import (
	"testing"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/retry"
)

func TestPolicy_ComputeBackoff_NoJitter(t *testing.T) {
	policy := retry.NewPolicy(1*time.Second, 10*time.Second, 2.0, retry.JitterNone)

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{attempt: 0, expected: 1 * time.Second},  // clamped to 1
		{attempt: 1, expected: 1 * time.Second},  // 1 * 2^0 = 1s
		{attempt: 2, expected: 2 * time.Second},  // 1 * 2^1 = 2s
		{attempt: 3, expected: 4 * time.Second},  // 1 * 2^2 = 4s
		{attempt: 4, expected: 8 * time.Second},  // 1 * 2^3 = 8s
		{attempt: 5, expected: 10 * time.Second}, // capped at max (10s)
		{attempt: 10, expected: 10 * time.Second},
	}

	for _, tt := range tests {
		actual := policy.ComputeBackoff(tt.attempt)
		if actual != tt.expected {
			t.Errorf("attempt %d: expected %v, got %v", tt.attempt, tt.expected, actual)
		}
	}
}

func TestPolicy_ComputeBackoff_FullJitterBounds(t *testing.T) {
	policy := retry.NewPolicy(1*time.Second, 16*time.Second, 2.0, retry.JitterFull)

	for attempt := 1; attempt <= 6; attempt++ {
		for iter := 0; iter < 50; iter++ {
			backoff := policy.ComputeBackoff(attempt)
			if backoff < 0 {
				t.Fatalf("attempt %d: backoff cannot be negative, got %v", attempt, backoff)
			}
			if backoff > 16*time.Second {
				t.Fatalf("attempt %d: backoff exceeded max interval, got %v", attempt, backoff)
			}
		}
	}
}

func TestPolicy_ComputeBackoff_EqualJitterBounds(t *testing.T) {
	policy := retry.NewPolicy(2*time.Second, 10*time.Second, 2.0, retry.JitterEqual)

	// For attempt 1: temp = 2s, half = 1s, backoff in [1s, 2s]
	for iter := 0; iter < 50; iter++ {
		backoff := policy.ComputeBackoff(1)
		if backoff < 1*time.Second || backoff > 2*time.Second {
			t.Fatalf("attempt 1 equal jitter out of bounds [1s, 2s]: got %v", backoff)
		}
	}
}
