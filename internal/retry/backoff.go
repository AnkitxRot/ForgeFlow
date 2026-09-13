package retry

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

// JitterStrategy defines how randomness is blended into backoff calculation.
type JitterStrategy string

const (
	JitterNone  JitterStrategy = "NONE"
	JitterFull  JitterStrategy = "FULL"  // sleep = rand(0, temp)
	JitterEqual JitterStrategy = "EQUAL" // sleep = temp/2 + rand(0, temp/2)
)

// Policy defines parameters for exponential retry backoff.
type Policy struct {
	BaseInterval time.Duration
	MaxInterval  time.Duration
	Multiplier   float64
	Jitter       JitterStrategy

	mu  sync.Mutex
	rng *rand.Rand
}

// DefaultPolicy returns a standard production backoff policy:
// base = 1s, max = 300s, multiplier = 2.0, full jitter.
func DefaultPolicy() *Policy {
	return NewPolicy(1*time.Second, 300*time.Second, 2.0, JitterFull)
}

// NewPolicy creates and validates a retry backoff policy.
func NewPolicy(base, max time.Duration, multiplier float64, jitter JitterStrategy) *Policy {
	if base <= 0 {
		base = 1 * time.Second
	}
	if max < base {
		max = base
	}
	if multiplier < 1.0 {
		multiplier = 2.0
	}
	if jitter == "" {
		jitter = JitterFull
	}

	return &Policy{
		BaseInterval: base,
		MaxInterval:  max,
		Multiplier:   multiplier,
		Jitter:       jitter,
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// ComputeBackoff calculates the retry backoff duration for a given execution attempt.
// Invariant: attempt >= 1.
// Formula: temp = min(MaxInterval, BaseInterval * Multiplier^(attempt - 1))
func (p *Policy) ComputeBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	// 1. Compute raw exponential backoff
	factor := math.Pow(p.Multiplier, float64(attempt-1))
	rawFloat := float64(p.BaseInterval) * factor

	var temp time.Duration
	if rawFloat >= float64(p.MaxInterval) || math.IsInf(rawFloat, 0) {
		temp = p.MaxInterval
	} else {
		temp = time.Duration(rawFloat)
	}

	// 2. Apply chosen jitter strategy
	switch p.Jitter {
	case JitterNone:
		return temp

	case JitterEqual:
		half := temp / 2
		randomPart := p.randomDuration(temp - half)
		return half + randomPart

	case JitterFull:
		fallthrough
	default:
		return p.randomDuration(temp)
	}
}

func (p *Policy) randomDuration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	n := p.rng.Int63n(int64(max))
	return time.Duration(n)
}
