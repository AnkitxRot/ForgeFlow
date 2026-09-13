package domain

import (
	"errors"
	"time"
)

type TenantStatus string

const (
	TenantStatusActive    TenantStatus = "ACTIVE"
	TenantStatusSuspended TenantStatus = "SUSPENDED"
)

// Tenant represents an isolated tenant namespace within ForgeFlow.
type Tenant struct {
	ID        string       `json:"id"`
	Name      string       `json:"name"`
	Status    TenantStatus `json:"status"`
	CreatedAt time.Time    `json:"created_at"`
}

func (t *Tenant) Validate() error {
	if t.ID == "" {
		return ErrEmptyTenantID
	}
	if t.Name == "" {
		return errors.New("tenant name must not be empty")
	}
	if t.Status == "" {
		t.Status = TenantStatusActive
	}
	return nil
}

// Queue represents a named queue partition within a tenant.
type Queue struct {
	TenantID         string    `json:"tenant_id"`
	Name             string    `json:"name"`
	IsPaused         bool      `json:"is_paused"`
	ConcurrencyLimit int       `json:"concurrency_limit"` // 0 = unlimited
	CreatedAt        time.Time `json:"created_at"`
}

func (q *Queue) Validate() error {
	if q.TenantID == "" {
		return ErrEmptyTenantID
	}
	if q.Name == "" {
		return ErrEmptyQueue
	}
	if q.ConcurrencyLimit < 0 {
		q.ConcurrencyLimit = 0
	}
	return nil
}
