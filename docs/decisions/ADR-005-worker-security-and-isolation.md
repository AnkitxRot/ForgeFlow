# ADR-005: Worker Security, Isolation Boundaries, and Code Execution

- **Status**: Accepted
- **Date**: 2026-09-13
- **Deciders**: Principal Systems Architect

## Context

Job and workflow engines execute tasks provided by users and systems. Execution mechanisms typically fall into two categories:
1. **In-Process Handlers (Application Workers)**:
   - Compiled functions registered within the worker binary (e.g. Go handler functions implementing `func(ctx context.Context, job Job) (Result, error)`).
   - Suitable for trusted microservices within the same administrative domain.
2. **Out-of-Process / Generic Command Execution**:
   - Running arbitrary shell commands, scripts (Bash, Python), binaries, or Docker/OCI containers.
   - If an untrusted or multi-tenant system allows users to submit shell commands (`rm -rf /`, `curl metadata-service`, cryptominers, fork bombs), arbitrary code execution poses catastrophic security risks:
     - Host compromise / root escape.
     - Access to the worker's filesystem, environment variables, database credentials, and cloud metadata APIs (`169.254.169.254`).
     - Denial of service via memory/CPU exhaustion.

## Decision

We establish an explicit **Security Isolation Hierarchy**:

### 1. Worker Execution Modes
The worker platform supports two strictly decoupled execution tiers:
- **Tier 1: Internal Registered Handlers (`HandlerRegistry`)**:
  - Handlers are pre-registered Go types compiled into the worker binary.
  - Job payloads provide structured JSON arguments validated against JSON Schema.
  - Handlers do NOT execute arbitrary shell scripts.
- **Tier 2: Isolated Process Runner (`SubprocessRunner`)**:
  - For jobs requesting CLI/binary invocation (e.g., executing a script or container):
  - **No Shell Execution**: Processes are invoked with direct `execve` (e.g. `[binary, arg1, arg2]`), strictly avoiding `sh -c` or `cmd.exe /c` to prevent shell injection vulnerabilities.
  - **Environment Sanitization**: Worker environment variables (especially DB credentials, API keys, AWS/cloud tokens) are stripped. Only an explicit, allowlisted set of environment variables is passed to child processes.
  - **OS Resource Limits**:
    - Linux: POSIX `setrlimit` or cgroups v2 (CPU quotas, memory limits, process limits to prevent fork bombs).
    - Windows: Win32 Job Objects (`AssignProcessToJobObject` with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, memory limit flags).
  - **Execution Timeouts**: Hardware / OS-level process group termination (`kill -9 -PGID` or `TerminateJobObject`) when execution deadline is exceeded.
  - **No Host Root**: Workers run under unprivileged, dedicated user accounts (e.g. `forgeflow-worker`).

### 2. Network Isolation and SSRF Protection
- Worker processes executing arbitrary user code must be blocked from accessing cloud metadata services (`http://169.254.169.254`, `http://metadata.google.internal`) and internal cluster networks via local firewall/routing rules (e.g. iptables or Windows filtering platform).

### 3. Tenant Boundary & Authorization
- Every API request is authenticated via API Keys / Bearer Tokens hashed with Argon2id.
- Tenant ID is resolved from the authenticated token and embedded in all database queries (`WHERE tenant_id = $1`). Cross-tenant access is structurally impossible at the SQL query layer.
- Workers authenticate with dedicated worker tokens scoped strictly to their assigned queues (`role: worker`). Workers cannot read or modify jobs belonging to other queues or submit administrative commands.

## Consequences

### Positive
- **Guaranteed Isolation**: Clear boundaries between trusted internal application jobs and untrusted arbitrary scripts.
- **No Accidental Credential Leakage**: Child processes cannot read host environment variables containing database passwords or master encryption keys.
- **Resilient Termination**: Unresponsive or runaway jobs are reliably killed at the OS process-group level, preventing orphaned background zombie processes.

### Negative / Trade-offs
- Full OCI container sandboxing (e.g. Firecracker microVMs or gVisor) is deferred to advanced deployment configurations rather than baked into the default single-binary distribution.

## Revisit Conditions
- If the platform is deployed as a public untrusted multi-tenant SaaS running arbitrary untrusted user-submitted code, requiring microVM isolation (Firecracker) per task.
