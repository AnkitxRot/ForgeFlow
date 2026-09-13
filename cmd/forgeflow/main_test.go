package main

import (
	"testing"

	"github.com/AnkitxRot/ForgeFlow/internal/version"
)

func TestMainSmoke(t *testing.T) {
	if version.Version == "" {
		t.Fatal("expected non-empty version")
	}
}
