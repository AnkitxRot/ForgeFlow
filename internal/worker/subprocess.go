package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/AnkitxRot/ForgeFlow/internal/domain"
)

// DefaultMaxOutputBytes limits stdout/stderr capture to 1MB to prevent OOM.
const DefaultMaxOutputBytes = 1024 * 1024

var (
	ErrBinaryNotFound      = errors.New("subprocess binary not found or not executable")
	ErrOutputLimitExceeded = errors.New("subprocess output exceeded maximum allowed size")
)

// SubprocessConfig defines parameters for executing a task as an isolated subprocess.
type SubprocessConfig struct {
	// BinaryPath is the absolute or resolved path to the executable. Direct invocation only, no shell wrapper.
	BinaryPath string
	// DefaultArgs are arguments prepended to any dynamic arguments.
	DefaultArgs []string
	// AllowedEnvKeys specifies environment variable names inherited from the host environment.
	AllowedEnvKeys []string
	// CustomEnv specifies additional KEY=VALUE environment pairs provided to the child.
	CustomEnv map[string]string
	// WorkingDir sets the working directory for the process.
	WorkingDir string
	// MaxOutputBytes limits the captured stdout/stderr buffer. Defaults to 1MB if 0.
	MaxOutputBytes int
}

// SubprocessHandler implements Handler by launching a subprocess per ADR-005.
type SubprocessHandler struct {
	cfg SubprocessConfig
}

// NewSubprocessHandler creates a validated SubprocessHandler.
func NewSubprocessHandler(cfg SubprocessConfig) (*SubprocessHandler, error) {
	if cfg.BinaryPath == "" {
		return nil, errors.New("binary path must not be empty")
	}
	if cfg.MaxOutputBytes <= 0 {
		cfg.MaxOutputBytes = DefaultMaxOutputBytes
	}
	// Verify the binary exists on PATH or filesystem
	resolved, err := exec.LookPath(cfg.BinaryPath)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrBinaryNotFound, cfg.BinaryPath)
	}
	cfg.BinaryPath = resolved

	return &SubprocessHandler{cfg: cfg}, nil
}

// Execute runs the binary with the job's payload supplied over stdin, capturing stdout as result.
func (h *SubprocessHandler) Execute(ctx context.Context, job *domain.Job) ([]byte, error) {
	cmd := exec.CommandContext(ctx, h.cfg.BinaryPath, h.cfg.DefaultArgs...)

	if h.cfg.WorkingDir != "" {
		cmd.Dir = h.cfg.WorkingDir
	}

	// 1. Construct stripped, whitelisted environment per ADR-005
	cmd.Env = h.buildSanitizedEnv()

	// 2. Supply payload to child stdin
	if len(job.Payload) > 0 {
		cmd.Stdin = bytes.NewReader(job.Payload)
	}

	// 3. Capture stdout & stderr with strict size bounds
	var stdoutBuf, stderrBuf bytes.Buffer
	limitedStdout := &limitedWriter{w: &stdoutBuf, maxBytes: h.cfg.MaxOutputBytes}
	limitedStderr := &limitedWriter{w: &stderrBuf, maxBytes: h.cfg.MaxOutputBytes}

	cmd.Stdout = limitedStdout
	cmd.Stderr = limitedStderr

	cmd.WaitDelay = 3 * time.Second

	// 4. Execute process
	err := cmd.Run()
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("subprocess cancelled: %w", ctx.Err())
		}
		if limitedStdout.exceeded || limitedStderr.exceeded {
			return nil, ErrOutputLimitExceeded
		}
		errMsg := strings.TrimSpace(stderrBuf.String())
		if errMsg != "" {
			return nil, fmt.Errorf("subprocess exited with error: %v (stderr: %s)", err, errMsg)
		}
		return nil, fmt.Errorf("subprocess exited with error: %w", err)
	}

	return stdoutBuf.Bytes(), nil
}

func isSensitiveEnvKey(key string) bool {
	upper := strings.ToUpper(key)
	sensitivePrefixes := []string{
		"AWS_", "AZURE_", "GOOGLE_", "GCP_", "DATABASE_", "PG", "POSTGRES_",
	}
	for _, p := range sensitivePrefixes {
		if strings.HasPrefix(upper, p) {
			return true
		}
	}
	sensitiveSubstrings := []string{
		"TOKEN", "SECRET", "PASSWORD", "API_KEY", "APIKEY", "CREDENTIAL", "AUTH",
	}
	for _, s := range sensitiveSubstrings {
		if strings.Contains(upper, s) {
			return true
		}
	}
	return false
}

// buildSanitizedEnv whitelists standard harmless environment keys and adds explicit non-sensitive custom keys.
func (h *SubprocessHandler) buildSanitizedEnv() []string {
	whitelisted := []string{
		"PATH", "TMP", "TEMP", "SYSTEMROOT", "USER", "HOME", "LANG", "LC_ALL",
	}
	for _, k := range h.cfg.AllowedEnvKeys {
		if !isSensitiveEnvKey(k) {
			whitelisted = append(whitelisted, k)
		}
	}

	envMap := make(map[string]string)
	for _, key := range whitelisted {
		if val, exists := os.LookupEnv(key); exists {
			envMap[key] = val
		}
	}

	for k, v := range h.cfg.CustomEnv {
		if !isSensitiveEnvKey(k) {
			envMap[k] = v
		}
	}

	var env []string
	for k, v := range envMap {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	return env
}

type limitedWriter struct {
	w        io.Writer
	written  int
	maxBytes int
	exceeded bool
}

func (lw *limitedWriter) Write(p []byte) (n int, err error) {
	if lw.written >= lw.maxBytes {
		lw.exceeded = true
		return len(p), nil // drop silently without erroring stream early
	}
	remaining := lw.maxBytes - lw.written
	if len(p) > remaining {
		lw.exceeded = true
		n, err = lw.w.Write(p[:remaining])
		lw.written += n
		return len(p), err
	}
	n, err = lw.w.Write(p)
	lw.written += n
	return n, err
}
