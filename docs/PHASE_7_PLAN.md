# Phase 7: Security and operational readiness

Status: **COMPLETED**

Phase 7 turns the distributed runtime completed in Phases 1-6 into a deployable, security-bounded service. Work remains incremental so identity, tenancy, secrets, delivery, and recovery each have an independently testable boundary.

## Security model

- Production API requests use OAuth 2.0 bearer tokens validated as JWTs against configured OpenID Connect issuer and JWK-set endpoints; startup does not depend on discovery-network availability.
- Authorization is deny-by-default and uses the bounded roles `VIEWER`, `OPERATOR`, `ADMIN`, and `MONITOR`.
- Authentication proves caller identity; tenant membership and tenant-scoped repository predicates are added separately in Slice 7.2 so authorization cannot be mistaken for data isolation.
- Health probes remain anonymous. Metrics require `MONITOR` or `ADMIN`; other actuator endpoints require `ADMIN`.
- Local development keeps security explicitly disabled by default. The production profile enables it and refuses to start without issuer configuration.

## Incremental implementation

### Slice 7.1: Authentication and role-based authorization

Status: **COMPLETED**

- Add Spring Security OAuth 2.0 resource-server support.
- Validate JWT issuer, subject, and bounded role claims without accepting caller-supplied identity headers.
- Enforce read, operate, administer, and monitoring permissions at the HTTP boundary.
- Return stable problem responses for unauthenticated and forbidden requests.
- Keep load-test topology security opt-out explicit until an identity provider is part of the harness.

Exit: security contract tests prove anonymous rejection, role boundaries, protected metrics, public probes, and fail-closed production configuration.

Implemented:

- Added Spring Security resource-server support with stateless sessions, disabled browser authentication flows, no request cache, and deny-by-default routing.
- Production validates signed JWTs against an explicit JWK endpoint and validates the configured issuer without requiring live discovery during startup.
- Added bounded `VIEWER`, `OPERATOR`, `ADMIN`, and `MONITOR` role conversion while preserving standard OAuth scopes and ignoring unknown role values.
- Added stable RFC 9457-style authentication and authorization problem responses without exposing decoder or policy internals.
- Kept health probes anonymous, restricted metrics to monitoring/admin callers, and separated read, execute, and administrative API permissions.
- The managed Phase 6 load topology explicitly disables security until a test identity provider is introduced, preventing an implicit production bypass.
- Verified the complete Maven reactor: 220 tests across 70 suites, with no failures, errors, or skipped tests.

Next: **Slice 7.2 - Tenant isolation and quotas**

### Slice 7.2: Tenant isolation and quotas

Status: **COMPLETED**

- Introduce immutable tenant identity in workflows, executions, schedules, inboxes, outboxes, and coordination keys.
- Derive tenant identity only from validated authentication claims.
- Enforce tenant predicates in every repository query and uniqueness constraint.
- Add tenant-aware quotas and cross-tenant negative integration tests.

#### Slice 7.2a: Tenant identity and schema foundation

Status: **COMPLETED**

- Add a strict tenant identifier value object and derive request tenancy only from validated JWT claims.
- Reject authenticated API calls with missing or invalid tenant claims; never accept tenant identity from request headers.
- Scope tenant identity into request logs and expose it to controllers through a leak-safe request attribute.
- Add an expand/backfill database migration for the tenant registry and durable workflow, execution, and schedule ownership roots.

Exit: identity and migration tests prove strict claim validation, header non-spoofing, local-development tenancy, cleanup, and backward-compatible ownership backfill.

Implemented:

- Added a framework-free `TenantId` value object with strict lowercase DNS-label validation and a 63-character bound.
- Added a security-chain tenant filter that derives identity only from the configured JWT claim, rejects missing/invalid claims, ignores spoofable headers, and clears request/MDC state after every request.
- Added an explicit configurable local tenant only for security-disabled development and load testing.
- Added V13 with a tenant registry plus indexed, non-null ownership on workflow, execution, and schedule roots.
- Kept a documented transitional `local` database default for rolling compatibility; Slice 7.2b removes it after all writers supply ownership explicitly.
- Added a real populated V12-to-V13 Testcontainers upgrade test and default-writer integration coverage.
- Verified the complete Maven reactor: 228 tests across 73 suites, with no failures, errors, or skipped tests.

Next: **Slice 7.2b - Tenant-scoped domain and persistence contracts**

#### Slice 7.2b: Tenant-scoped domain and persistence contracts

Status: **COMPLETED**

- Carry tenant identity explicitly through controllers, application use cases, aggregates, repository ports, event contracts, inboxes, outboxes, and Redis keys.
- Add tenant predicates to every externally addressable read and mutation, including idempotency and schedule operations.
- Remove transitional database defaults after every writer supplies explicit ownership.
- Prove that known identifiers from another tenant produce not-found behavior and no mutation.

##### Slice 7.2b1: Workflow-definition ownership and isolation

Status: **COMPLETED**

- Carry `TenantId` through the workflow aggregate, controller, application service, and repository port.
- Write workflow ownership explicitly and predicate create/read/list/update/publish/archive persistence paths by tenant.
- Return the owning tenant in workflow API representations and make cross-tenant identifiers indistinguishable from missing workflows.
- Keep legacy internal execution and scheduling callers on an explicit `local` compatibility overload until their own tenant-scoped slices migrate them.
- Prove two-tenant list isolation and rejected cross-tenant reads and mutations against PostgreSQL.

Exit: the workflow-definition API and persistence adapter cannot read or mutate another tenant's workflows, and the complete Maven reactor remains green.

Implemented:

- Added tenant-aware workflow repository and service contracts while retaining deterministic local-only compatibility overloads for unmigrated internal callers.
- Added tenant ownership to `WorkflowDefinition`, explicit tenant writes, tenant-index-aligned query predicates, and tenant-scoped pessimistic locking.
- Derived workflow API tenancy from the authenticated request context and exposed the tenant in workflow responses.
- Added PostgreSQL negative integration coverage for cross-tenant get, list, update, publish, and archive behavior.
- Verified the complete Maven reactor: 226 tests across 72 suites, with no failures, errors, or skipped tests.

Next: **Slice 7.2b2 - Execution, event, and messaging isolation**

##### Slice 7.2b2: Execution, event, and messaging isolation

Status: **COMPLETED**

- Carry tenant identity through execution aggregates, commands, Kafka contracts, inboxes, outboxes, retries, heartbeats, and dead-letter paths.
- Scope execution idempotency and every execution/event persistence query by tenant.
- Prove cross-tenant execution identifiers and idempotency keys cannot observe or mutate foreign state.

###### Slice 7.2b2a: Execution ownership and API isolation

Status: **COMPLETED**

- Carry `TenantId` through execution aggregates, HTTP representations, application services, and repository ports.
- Scope execution start, idempotency replay, lookup, cancellation, and published-workflow admission by tenant.
- Enforce execution-to-workflow ownership with a composite PostgreSQL foreign key and tenant-scoped idempotency uniqueness.
- Prove stable same-tenant replay plus rejected cross-tenant start, read, cancellation, and mismatched persistence.

Implemented:

- Added tenant-aware execution contracts while retaining deterministic local-only compatibility overloads for internal paths not yet migrated.
- Added explicit tenant writes and predicates across externally addressable execution persistence operations.
- Added V14 ownership constraints that prevent assigning an execution to a workflow owned by another tenant.
- Added populated-schema V12-to-V14 migration coverage and two-tenant PostgreSQL negative tests.
- Verified the complete Maven reactor: 227 tests across 72 suites, with no failures, errors, or skipped tests.

Next: **Slice 7.2b2b - Event and messaging tenant propagation**

###### Slice 7.2b2b: Event and messaging tenant propagation

Status: **COMPLETED**

- Carry tenant identity through task commands, results, heartbeats, execution events, Kafka headers/contracts, and dead-letter records.
- Persist and predicate tenant ownership in control-plane and worker inboxes and outboxes.
- Prove duplicate delivery, retry, replay, and failure recovery cannot cross tenant boundaries.

Implemented:

- Added tenant identity to the common message envelope with explicit legacy V1 records mapped to the `local` tenant.
- Added exact Kafka tenant-header validation on commands, results, and heartbeats, while preserving tenant headers through dead-letter publication and replay.
- Added control-plane V15 ownership for execution events, inboxes, and outboxes, including tenant-scoped uniqueness and execution foreign keys.
- Added worker V3 ownership for command inboxes and result outboxes, including tenant-scoped command deduplication and deterministic result identities.
- Added PostgreSQL and Kafka tests proving foreign-tenant results cannot advance executions, equal event IDs remain isolated across tenants, and DLQ recovery preserves tenant identity.
- Verified the complete Maven reactor: 232 tests across 73 suites, with no failures, errors, or skipped tests.

Next: **Slice 7.2b3 - Schedule, coordination, and migration closeout**

##### Slice 7.2b3: Schedule, coordination, and migration closeout

Status: **COMPLETED**

- Tenant-scope schedules, trigger leases, Redis coordination keys, permit ledgers, and backpressure controls.
- Remove local compatibility overloads and transitional database defaults after every writer supplies explicit ownership.
- Run the complete cross-tenant failure and concurrency matrix before closing Slice 7.2b.

Implemented:

- Added explicit tenant ownership to schedule aggregates, APIs, application services, repositories, trigger materialization, lease claims, and deterministic execution admission.
- Partitioned pending-schedule and ready-task discovery by tenant, with tenant-scoped PostgreSQL advisory locks so one tenant cannot consume another tenant's admission capacity.
- Added tenant identity to coordination permits and token buckets, including tenant-scoped durable keys, Redis keyspaces, recovery, expiry, and reconciliation.
- Added V16 composite ownership constraints and indexes for schedules, triggers, concurrency resources, permits, and admission buckets, then removed the transitional ownership defaults from durable roots.
- Added populated-schema and fresh-schema migration verification plus two-tenant PostgreSQL and Redis tests for schedule isolation, capacity independence, lease fencing, coordination isolation, and key separation.
- Removed local compatibility overloads from workflow, execution, scheduling, and coordination use cases; legacy V1 wire records retain an explicit `local` mapping only at their compatibility boundary.
- Verified the complete Maven reactor: 235 tests across 73 suites, with no failures, errors, or skipped tests.

Next: **Slice 7.2c - Tenant quotas and distributed verification**

#### Slice 7.2c: Tenant quotas and distributed verification

Status: **COMPLETED**

- Persist versioned per-tenant admission and resource quotas.
- Enforce quotas through PostgreSQL-authoritative coordination across replicas.
- Add concurrent cross-tenant, retry, failover, and Redis-loss verification.

Implemented:

- Added a validated tenant quota policy for active executions, running tasks, pending schedule fires, ready tasks, schedule admission rate, and task-dispatch rate.
- Added V17 durable tenant quotas with optimistic versions, ownership constraints, bounded values, and inherited deployment defaults for tenants without an override.
- Added tenant-context-only quota administration at `GET` and `PUT /api/v1/tenant/quota`, with ETags, mandatory `If-Match`, stale-writer fencing, and stable quota problem responses.
- Enforced active-execution, running-task, ready-queue, and pending-fire limits under tenant-scoped PostgreSQL locks so replicas cannot oversubscribe shared capacity.
- Resolved tenant rate policies consistently in eager dispatch, durable outbox dispatch, result-driven DAG advancement, and schedule firing while retaining PostgreSQL as the authoritative token state.
- Added concurrent two-tenant admission tests, stale quota writer races, independent schedule capacity tests, and total Redis-loss verification.
- Verified the complete Maven reactor: 244 tests across 76 suites, with no failures, errors, or skipped tests.

Next: **Slice 7.3 - Secret references, redaction, and audit trail**

### Slice 7.3: Secret references, redaction, and audit trail

Status: **COMPLETED**

- Replace inline task secrets with provider-neutral secret references resolved only by workers.
- Prevent secret material from entering API responses, logs, Kafka events, traces, or failure details.
- Persist append-only security and administrative audit events with actor, tenant, action, target, and outcome.
- Define retention and redaction policy tests.

Implemented:

- Added validated provider-neutral secret references as a dedicated task-definition field and recursively rejected inline password, secret, credential, private-key, API-key, and token configuration.
- Added V18 persistence for task secret references and propagated references through ready-task claiming and durable task-command outbox messages without introducing a resolved-value field.
- Added a worker secret-provider SPI, a local `env` provider, per-tenant reference resolution immediately before handler invocation, and cleanup of the short-lived resolved binding set.
- Added defense-in-depth sensitive-data redaction for resolved values and credential assignments before worker logs, observations, task-result events, and durable failure details.
- Added V18 append-only security audit storage with database-enforced update/delete rejection, indexed tenant/request history, and actor, tenant, action, target, outcome, status, request ID, and timestamp fields.
- Added fail-closed mutation admission with durable `ATTEMPTED` events and append-only `SUCCEEDED`, `DENIED`, or `FAILED` terminal events; request bodies, query strings, headers, exception details, and task configuration are excluded.
- Added tenant-scoped, administrator-only `GET /api/v1/audit-events` history and a configurable 365-day online retention window.
- Documented the secret, redaction, immutable archival, and retention contract in `docs/security/secret-and-audit-policy.md`.
- Verified the complete Maven reactor: 257 tests across 84 suites, with no failures, errors, or skipped tests.

Next: **Slice 7.4 - Container images and deployment manifests**

### Slice 7.4: Container images and deployment manifests

Status: **COMPLETED**

- Build minimal, non-root, reproducible control-plane and worker images.
- Add Kubernetes manifests or Helm packaging with probes, disruption budgets, resource limits, topology spread, and restricted security contexts.
- Externalize credentials and trust material through secret references.

Implemented:

- Added separate multi-stage control-plane and worker Dockerfiles using digest-pinned, multi-architecture Temurin Java 21 builder and Alpine JRE bases.
- Added commit-timestamp-driven reproducible Maven archive inputs, OCI version/revision/source metadata, BuildKit provenance, a bounded Docker context, and explicit Windows-to-Linux wrapper normalization.
- Added an image build script that derives Git metadata, supports local load or authenticated registry push, supplies corporate build trust as a non-output BuildKit secret, and rejects images that do not retain UID/GID 10001.
- Kept runtime images at 146 MB or less while using a non-root account, owned application artifact, container-aware JVM memory policy, liveness health check, and no build toolchain in the final stage.
- Added a versioned Helm application chart with independently scalable control-plane and worker Deployments, Services, ServiceAccount, PodDisruptionBudgets, and optional stabilization-window HPAs.
- Added zero-unavailable rolling updates, separate 45/90-second termination windows, pre-stop drains, startup/liveness/readiness probes, resource requests and limits, host/zone topology spread, and size-bounded writable `/tmp` volumes.
- Added pod and container restricted security controls: non-root UID/GID 10001, read-only root filesystems, disabled privilege escalation and service-account token mounts, dropped Linux capabilities, and RuntimeDefault seccomp.
- Externalized database credentials, Kafka SASL configuration, Redis credentials, OIDC trust endpoints, registry pulls, and optional PKCS12 trust material through existing Kubernetes Secret references; the chart never creates credential Secrets.
- Added deployment contract tests plus operator documentation for build trust, immutable image digests, capacity-aware autoscaling, atomic install, rollout verification, and rollback boundaries.
- Verified strict Helm lint, default and trust-store/HPA rendering, and Kubernetes client schema validation.
- Built both images through Rancher Desktop, verified Java 21.0.12, health checks, read-only-root compatibility, artifact readability, and UID/GID 10001; the control plane was 145,322,292 bytes and the worker 127,868,266 bytes.
- Verified the complete Maven reactor: 260 tests across 85 suites, with no failures, errors, or skipped tests.

Next: **Slice 7.5 - CI/CD quality and security gates**

### Slice 7.5: CI/CD quality and security gates

Status: **COMPLETED**

Implemented:

- Added pull-request and main-branch CI with independent fast-test, architecture, populated-schema migration, full distributed-integration, Kubernetes-manifest, dependency, SBOM, image-build, and vulnerability-scan gates.
- Added a single local quality-gate entry point with cross-platform Maven Wrapper selection and repeatable fast, architecture, migration, full, manifest, and SBOM modes.
- Added a manually approved release workflow that validates a semantic version, reruns the release acceptance gates, publishes immutable multi-platform images, generates registry-attached provenance and SBOMs, scans image digests, and signs and attests both images with protected credentials.
- Pinned every third-party GitHub Action to a full commit SHA and documented GitHub Enterprise action-mirroring, protected-environment, registry, signing-key, and branch-protection boundaries.
- Added CycloneDX 1.6 aggregate SBOM generation without a build-specific serial number and verified 156 runtime components.
- Added fixed HIGH/CRITICAL vulnerability enforcement with Trivy; upgraded embedded Tomcat to 11.0.25 and moved runtime images to a digest-pinned Ubuntu Noble JRE base so both final images scan clean.
- Added executable delivery-pipeline contract tests covering YAML parsing, required gates, immutable action references, release signing, and cross-platform automation.
- Re-ran the complete Rancher Desktop-backed Maven reactor after remediation: 264 tests across 86 suites with no failures, errors, or skipped tests.
- Re-ran the bounded distributed release smoke test: 25 of 25 workflows succeeded, durable PostgreSQL state reconciled to 25 successes, zero duplicate commands or errors were observed, and all correctness thresholds passed.

Next: **Slice 7.6 - Operational recovery and release closeout**

### Slice 7.6: Operational recovery and release closeout

Status: **COMPLETED**

#### Slice 7.6.1: Backup/restore and reconciliation

Status: **COMPLETED**

- Added a real PostgreSQL custom-format backup/transactional-restore integration test with a populated, multi-tenant recovery fixture.
- Validated the restored database against all 19 Flyway migrations and compared every public table count with the captured recovery point.
- Proved that post-snapshot writes are excluded while secret references, active durable permits, tenant ownership constraints, and append-only security auditing survive restoration.
- Added an independent `Recovery` quality gate that combines database restoration with PostgreSQL-authoritative Redis permit and rate-state reconstruction tests.
- Required the recovery gate in pull-request CI and before release publication.
- Added a conservative backup/restore runbook covering recovery objectives, encrypted backup evidence, isolated restoration, Kafka recovery-point boundaries, empty-Redis reconstruction, abort conditions, and approval evidence.
- Verified the complete Rancher Desktop-backed Maven reactor: 266 tests across 87 suites with no failures, errors, or skipped tests.

Next: **Slice 7.6.2 - WAL/PITR proof, schema rollback boundaries, and credential rotation**

#### Slice 7.6.2: WAL/PITR, rollback boundaries, and credential rotation

Status: **COMPLETED**

- Added a real PostgreSQL physical-recovery integration test with WAL archival, SHA256 base-backup manifest verification, a named restore point, `recovery.signal`, promotion onto a new timeline, and exclusion of a post-boundary write.
- Added an explicit V17-to-V18 compatibility test proving that the previous workflow-task SQL contract remains readable and writable after the additive secret-reference migration.
- Defined a forward-only schema policy: application images may roll back only across a tested compatibility boundary; applied Flyway migrations are never reversed during an application rollback.
- Added an opaque `global.rotationToken` Helm contract that restarts both workloads after externally managed credentials or trust material change without accepting secret values.
- Added database, Kafka, Redis, OIDC/JWKS, trust-store, registry, and signing-key rotation procedures with overlap, verification, abort, revocation, and evidence requirements.
- Expanded the independent `Recovery` gate to run logical restore, physical PITR, schema compatibility, and PostgreSQL-authoritative Redis reconstruction together.
- Rendered the rotation annotation path in strict Helm/Kubernetes manifest validation and added executable repository contract tests for the recovery and rotation procedures.
- Verified the complete Rancher Desktop-backed Maven reactor: 268 tests across 89 suites with no failures, errors, or skipped tests.

Next: **Slice 7.6.3 - Incident response and DLQ replay acceptance**

#### Slice 7.6.3: Incident response and tenant-safe DLQ replay

Status: **COMPLETED**

- Added administrator-only, tenant-bound inspection and replay endpoints addressing exactly one allowlisted DLQ topic, partition, and offset.
- Returned payload size and SHA-256 evidence without exposing the raw value, and made missing and foreign-tenant records indistinguishable.
- Added a PostgreSQL replay ledger with tenant-scoped idempotency, unique source-location claims, bounded publishing leases, failure evidence, and no payload/header storage.
- Replayed the original key/value to the mapped source partition after Kafka broker acknowledgement while preserving stable contract/trace headers and stripping DLQ-only metadata.
- Added explicit at-least-once recovery semantics for the Kafka-acknowledged/PostgreSQL-unconfirmed window and relied on stable event IDs plus downstream inbox constraints for duplicate safety.
- Added exact administrative audit targets, source-topic replay metrics, stable 404/409/503 API contracts, and fail-closed inspection behavior when Kafka is unavailable.
- Added an incident-response and replay runbook covering containment, approvals, single-record replay, abort criteria, monitoring, and payload-free closeout evidence.
- Expanded the independent `Recovery` gate with a real PostgreSQL/Kafka acceptance test proving cross-tenant hiding, same-partition replay, header sanitation, payload-free ledger storage, and idempotent duplicate suppression.
- Verified the complete Rancher Desktop-backed Maven reactor: 281 tests across 92 suites with no failures, errors, or skipped tests.

Next: **Slice 7.6.4 - Release acceptance matrix and ADR-007 closeout**

#### Slice 7.6.4: Release acceptance matrix and ADR-007 closeout

Status: **COMPLETED**

- Added independent `Security` and `Upgrade` quality gates to local automation, pull-request CI, and the protected release workflow.
- Added a fail-fast, one-command release-acceptance orchestrator that requires Security, Upgrade, Recovery, Full, Manifests, and Sbom to pass on the same revision.
- Recorded ADR-007 with the identity, tenant-isolation, secret, audit, durable-authority, recovery, artifact-trust, and protected-publication boundaries.
- Documented the repository and protected-publication acceptance matrices without treating a local run as authorization to publish or deploy.
- Added executable pipeline contracts covering the new gates, same-revision rule, and separation between local evidence and protected release authority.
- Passed the final Rancher Desktop-backed matrix: 24 security tests, two upgrade tests, thirteen recovery tests, strict Helm/kubectl validation, and a 156-component CycloneDX 1.6 SBOM.
- Verified the complete clean Maven reactor: 282 tests across 92 suites with no failures, errors, or skipped tests.

Phase 7 is complete. Publication remains an explicit, reviewed action through the protected release workflow.

Exit: a versioned release can be securely deployed, observed, upgraded, restored, and operated with tenant isolation and auditable administrative actions.

## Deliberately deferred

- A hosted identity provider, cloud secret manager, and cloud-specific database backup service remain deployment integrations; FlowForge defines and tests their contracts.
- Fine-grained per-workflow ACLs are deferred until tenant isolation and the bounded Phase 7 role model are complete.
