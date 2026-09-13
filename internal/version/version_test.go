package version_test

import (
	"testing"

	"github.com/AnkitxRot/ForgeFlow/internal/version"
)

func TestVersionNotEmpty(t *testing.T) {
	if version.Version == "" {
		t.Fatal("expected version.Version to be non-empty")
	}
}
