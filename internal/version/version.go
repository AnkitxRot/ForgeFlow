package version

// Version holds the current semantic version of ForgeFlow.
const Version = "0.1.0-dev"

// Commit holds the Git commit SHA, injected at build time via -ldflags.
var Commit = "unknown"

// BuildDate holds the build timestamp, injected at build time via -ldflags.
var BuildDate = "unknown"
