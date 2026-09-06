# ADR-007: Security, tenancy, and operational release boundary

## Status

Accepted

## Context

FlowForge executes tenant-owned workflows across HTTP, PostgreSQL, Kafka, Redis, schedulers, and independently scaled workers. Correct distributed execution is insufficient for production readiness if caller identity can be spoofed, tenant predicates are inconsistent, secrets enter durable diagnostics, privileged recovery is unaudited, artifacts are mutable, or a release can bypass upgrade and restore evidence.

Authentication alone does not prove tenant isolation. Redis cannot be a correctness authority. A successful application build does not prove schema rollback compatibility, recoverability, safe deployment manifests, image provenance, or operator response. Some publication controls also require protected external systems that cannot be honestly certified by a local test.

## Decision

### Identity, authorization, and tenant ownership

- Production accepts OAuth 2.0 JWT bearer identities only from the configured issuer and JWK set. Caller-supplied tenant headers are not identity.
- HTTP authorization is deny-by-default with bounded `VIEWER`, `OPERATOR`, `ADMIN`, and `MONITOR` roles. Health is public; metrics and administrative operations are explicitly protected.
- A validated tenant claim is carried through domain, application, persistence, scheduling, Kafka, worker, inbox, outbox, retry, heartbeat, audit, and recovery contracts.
- PostgreSQL queries and composite ownership constraints enforce tenant scope at the durable boundary. Foreign identifiers return not found where existence would disclose another tenant.

### Secrets and administrative evidence

- Workflow definitions carry provider-neutral secret references, never resolved secret values. Only workers resolve them at execution time.
- Inline-secret rejection and recursive redaction prevent resolved material from reaching logs, events, observations, durable failure details, or API responses.
- Administrative mutations and sensitive DLQ inspection produce append-only attempt/outcome audit records. Mutation and inspection fail closed if their audit attempt cannot be persisted.
- Online audit retention is bounded; production archival is immutable and deployment-specific. Audit and release evidence exclude payloads and credentials.

### Durable correctness and privileged recovery

- PostgreSQL is authoritative for workflow state, idempotency, leases, permits, rate state, replay claims, and audit history. Redis is an ephemeral acceleration layer reconstructed from PostgreSQL.
- Kafka delivery is at least once. Stable event IDs, transactional outboxes, durable inboxes, fencing tokens, and idempotent state transitions absorb duplicates and crash ambiguity.
- DLQ access is administrator-only, tenant-bound, allowlisted, single-record, same-partition, idempotency-keyed, broker-acknowledged, and audited. Inspection returns a payload digest and length but not the raw value.
- Applied Flyway migrations are forward-only. Application rollback is allowed only across an explicitly tested compatibility boundary. Logical restore and named-point PITR are independently tested.

### Deployment and artifact trust

- Control-plane and worker images are independently scalable, non-root, read-only, digest-deployed artifacts built from pinned Java 21 bases.
- Helm templates enforce restricted security contexts, probes, resources, disruption budgets, topology spread, graceful drains, external Secret references, and restart tokens that contain no secret material.
- Pull-request automation runs independent architecture, security, upgrade, recovery, full-reactor, manifest, SBOM, dependency, secret, configuration, and image checks.
- The protected release workflow alone may publish. It requires clean `main`, a semantic version, reviewer-controlled credentials, bounded distributed smoke acceptance, immutable image digests, maximal provenance, SBOMs, fixed HIGH/CRITICAL scanning, Cosign signatures, signed attestations, and immediate verification.

### Acceptance boundary

- `scripts/invoke-release-acceptance.ps1` composes the locally repeatable `Security`, `Upgrade`, `Recovery`, `Full`, `Manifests`, and `Sbom` gates and stops at the first failure.
- All repository rows must pass on the same revision. Environment-specific registry, signing, approval, and deployment controls are certified only by the protected release workflow and production change record.
- A release or recovery failure is not bypassed through manual database edits, offset advancement, mutable retagging, disabled scanning, or partial evidence from another revision.

## Consequences

FlowForge has defense in depth across authenticated requests, tenant-scoped code contracts, database constraints, message headers, worker persistence, and operator recovery. Privileged actions are attributable, duplicate delivery remains safe, ephemeral-state loss is recoverable, and releases have an explicit evidence chain from source revision to signed digest.

The design adds tenant columns and indexes, extra authorization and audit writes, provider integration work, longer CI/release times, and operational evidence retention. At-least-once delivery can still produce duplicates, telemetry can still be incomplete, and a local acceptance run cannot certify a hosted identity provider, cloud backup policy, registry permissions, signing-key custody, or production rollout. Those controls remain deployment responsibilities and must fail closed at their documented boundaries.

Fine-grained per-workflow ACLs, cloud-specific secret/backup adapters, multi-region disaster recovery, automated destructive rollback, and keyless workload-identity signing remain deliberate future work rather than implicit Phase 7 claims.

The executable matrix is documented in the [Phase 7 release acceptance](../operations/phase-7-release-acceptance.md) record. Recovery, rollback/rotation, and incident procedures are defined in their companion runbooks. Earlier distributed correctness and observability decisions remain governed by ADR-003 through ADR-006.
