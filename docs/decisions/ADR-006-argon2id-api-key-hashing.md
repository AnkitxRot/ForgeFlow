# ADR-006: Argon2id Cryptographic API Key Verification and Revocation

- **Status**: Accepted
- **Date**: 2026-09-13
- **Deciders**: Senior Distributed Systems & Security Engineers

## Context

ForgeFlow uses API keys (`X-API-Key` or `Authorization: Bearer`) for client authentication and multi-tenant isolation.
In earlier iterations, stored API key verifiers used unsalted SHA-256 (`sha256.Sum256`).

Fast cryptographic hashes such as SHA-256 are susceptible to high-speed offline attacks:
1. **GPU/ASIC Acceleration**: Modern hardware can compute billions of SHA-256 hashes per second, allowing rapid dictionary cracking or rainbow table lookup if a database snapshot is leaked.
2. **Missing Salt**: Unsalted hashes allow batch cracking of identical keys across tenants.
3. **Master Plan Alignment**: Section 19 (Phase 6) and ADR-005 explicitly mandate Argon2id API key verification.

## Decision

1. Mandate **Argon2id (RFC 9106)** for all API key hashing and verification.
2. Store key credentials using standard PHC string formatting:
   `$argon2id$v=19$m=65536,t=3,p=2$<salt-b64>$<hash-b64>`
3. Generate a cryptographically secure 16-byte random salt (`crypto/rand`) for every key registration.
4. Execute comparison in strictly constant time (`subtle.ConstantTimeCompare`) after deriving the candidate key.
5. Provide explicit key revocation (`RevokeKey`), ensuring revoked keys are rejected immediately with `401 Unauthorized`.

## Consequences

### Positive
- **Hardware Resistance**: Argon2id's memory hardness (64MB default in production) renders GPU and ASIC mass cracking economically unfeasible.
- **Zero Raw Key Storage**: The server never persists or logs raw API keys.
- **Constant-Time Verification**: Prevents timing side-channel attacks during key validation.
- **Revocation Safety**: Keys can be revoked dynamically without restarting the daemon.

### Negative / Trade-offs
- Verification requires CPU and memory overhead compared to plain SHA-256. For high-frequency API endpoints, client sessions or token caching can be layered if throughput warrants.
- Revocation scope: In-memory `Argon2KeyValidator` maintains process-local revocation. In a distributed multi-node API deployment, key revocation must be synchronized via a shared database table or cache.
