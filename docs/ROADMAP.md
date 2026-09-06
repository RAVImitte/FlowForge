# FlowForge roadmap

This document is the canonical implementation-status tracker for FlowForge. Update it when a phase begins or reaches its exit criteria.

## Status legend

- `COMPLETED`: implemented and verified
- `IN PROGRESS`: active development
- `PLANNED`: agreed scope, not started

## Phase summary

| Phase | Status | Scope |
|---|---|---|
| Phase 1 | COMPLETED | Project foundation and versioned workflow definitions |
| Phase 2 | COMPLETED | Workflow state machine, execution, and DAG resolution |
| Phase 3 | COMPLETED | Kafka messaging, transactional outbox, and distributed workers |
| Phase 4 | COMPLETED | Retries, timeouts, dead-letter queues, and failure recovery |
| Phase 5 | COMPLETED | Durable scheduling, Redis coordination, and backpressure |
| Phase 6 | COMPLETED | Observability, scalability validation, and resilience testing |
| Phase 7 | COMPLETED | Security, delivery automation, and operational readiness |

## Phase 1: Foundation and workflow definitions

Status: **COMPLETED**

Completed capabilities:

- Java 21 and Spring Boot multi-module project
- Clean domain, application, and adapter boundaries
- PostgreSQL local environment through Docker Compose
- Flyway-managed workflow schema
- Versioned workflow definitions and immutable publication
- Task and dependency persistence using JSONB configuration
- DAG validation, including cycle, duplicate, and reference checks
- Create, read, list, update, publish, and archive APIs
- Strong ETag and `If-Match` optimistic concurrency
- Problem Details API errors
- Actuator health and metrics endpoints
- Domain, HTTP, persistence, migration, and architecture tests
- Maven wrapper and continuous-integration workflow
- Phase 1 architecture decision record and usage documentation

Verification notes:

- The Maven reactor builds successfully.
- Both Phase 1 PostgreSQL Testcontainers tests pass locally against Rancher Desktop's Moby engine.

## Phase 2: State machine and DAG execution

Status: **COMPLETED**

Completed capabilities:

- Immutable `WorkflowRun` and `TaskRun` domain aggregates
- Explicit, guarded workflow and task transition matrices
- Terminal-state, cancellation, timeout, timestamp, and optimistic state-version invariants
- Focused domain tests for valid and invalid transitions
- Flyway-managed `workflow_execution`, `task_execution`, `task_attempt`, and ordered `execution_event` persistence
- Immutable references from executions to the exact published workflow version
- Idempotent execution starts enforced by PostgreSQL
- Linear, fan-out, fan-in, and mixed-DAG readiness resolution
- Bounded, horizontally safe ready-task claims using `FOR UPDATE SKIP LOCKED`
- Workflow-scoped locking for concurrent and duplicate completion safety
- Failure propagation and coordinated workflow cancellation
- Start, inspect, and cancel execution APIs
- In-process dispatcher port with deterministic `NOOP`, `DELAY`, and `FAIL` handlers
- Startup and scheduled recovery of durable ready work
- Domain, application, web, handler, recovery, persistence, and concurrency tests

Verification notes:

- The complete Maven reactor verification succeeds with 43 tests passing and no skipped tests.
- All eight PostgreSQL Testcontainers tests pass locally against Rancher Desktop's Moby engine, including the six Phase 2 execution and concurrency scenarios.
- Kafka, distributed workers, retries, leases, and Redis remain intentionally deferred to later phases.

Planned scope:

- Persistent `workflow_execution`, `task_execution`, `task_attempt`, and execution-event models
- Explicit workflow and task state machines with guarded transitions
- Start, inspect, and cancel workflow executions
- Materialize executions from an immutable published workflow version
- Resolve root tasks, fan-out, fan-in, and mixed DAG dependencies
- Make dependent tasks ready only after every prerequisite succeeds
- Support concurrent completion events without duplicate transitions
- Add an in-process task dispatcher behind an application port
- Provide deterministic `NOOP`, `DELAY`, and `FAIL` task handlers for testing
- Recover durable ready work after an application restart
- Add execution history and state-transition tests

Exit criteria:

- Linear, fan-out, fan-in, failure, and cancellation workflows behave deterministically.
- State transitions are durable and concurrency-safe.
- Duplicate task completion cannot advance the DAG twice.
- Executions always reference an immutable published definition version.
- The full Maven verification suite passes.
- Kafka and Redis remain outside this phase.

## Phase 3: Event-driven execution and distributed workers

Status: **COMPLETED**

Completed milestones: **Slices 3.1-3.5 - Kafka foundation through production cutover**

Detailed implementation plan: [Phase 3 plan](PHASE_3_PLAN.md)

Completed capabilities:

- Framework-light `flowforge-messaging` module with immutable V1 envelopes and payload contracts
- Versioned task-command, task-result, and execution-event topic names
- JSON round-trip and backward-compatibility fixture tests
- Spring-managed Kafka topic provisioning with configurable partitions and replication factor
- Single-node Kafka 4.3 KRaft local environment
- Independently deployable `flowforge-worker` Spring Boot module
- Worker identity and live Kafka cluster health indicators
- Real Kafka integration tests for topic provisioning and worker connectivity
- Flyway-managed control-plane transactional outbox
- Atomic task-command and execution-event creation alongside state transitions
- Stable event IDs reused from the durable execution-event journal
- Bounded multi-instance claims using `FOR UPDATE SKIP LOCKED`
- Short claim leases and token-fenced acknowledgements for publisher crash recovery
- Kafka publisher with idempotent producer configuration, correlation headers, and explicit acknowledgements
- Startup and scheduled recovery of pending or expired outbox work
- Metrics for publication, failure, uncertain/stale acknowledgements, and pending backlog
- PostgreSQL concurrency tests and end-to-end PostgreSQL-to-Kafka publication tests
- Durable worker command inbox persisted before handler execution
- Worker-side application ports with deterministic `NOOP`, `DELAY`, and `FAIL` handlers
- Manual Kafka acknowledgement only after durable command completion
- Atomic worker completion and task-result outbox creation
- Scheduled task-result publication with stable event IDs and recoverable claim leases
- Completed-command deduplication that reuses the stored result without rerunning the handler
- Kafka consumer-group coordination verified with two live workers sharing six partitions
- Flyway-managed control-plane result inbox with event-ID uniqueness and payload reuse detection
- Manual result-consumer acknowledgement only after the PostgreSQL transaction commits
- Atomic result-inbox insertion, task completion, DAG release, lifecycle events, and downstream command creation
- Workflow-scoped downstream claims that cannot consume another workflow's dispatch capacity
- Stale state-version, task-identity, attempt, and conflicting event rejection
- Idempotent handling for repeated event IDs and independently duplicated equivalent results
- Result-ingestion metrics for applied, redundant, duplicate, and failed records
- Real Kafka fan-out/fan-in completion through the result consumer
- Production profile that selects the complete distributed topology and disables in-process dispatch
- Startup validation for conflicting, incomplete, or Kafka-less distributed configurations
- Independent Flyway histories that support either service starting first against a shared schema
- Cross-process restart recovery for pending control-plane commands, worker results, and offline result consumers
- ADR-003 documentation of delivery guarantees, partition keys, scaling limits, cutover, and failure semantics

Verification notes:

- The complete Maven reactor succeeds with 88 tests passing and no skipped tests.
- PostgreSQL tests verify atomic state/outbox writes, expired-lease recovery, token fencing, and disjoint concurrent claims.
- Kafka tests verify real command/event publication, keys, headers, versioned payloads, acknowledgements, and durable published state.
- Worker tests verify duplicate command suppression, durable result publication, uncertain-ack recovery, and two-instance partition sharing.
- Result-ingestion tests verify atomic DAG advancement, stale-result rollback, concurrent deduplication, failure, cancellation, and Kafka fan-out/fan-in completion.
- Restart testing verifies pending control-plane commands, durable worker results, and offline result consumption across replacement processes.

Planned scope:

- Kafka task-command and lifecycle-event topics
- Transactional outbox for PostgreSQL-to-Kafka delivery
- Consumer inbox and event-ID deduplication
- Independently deployable worker process
- Kafka consumer-group worker coordination
- Versioned event contracts and compatibility tests
- At-least-once delivery with idempotent processing

## Phase 4: Reliability and failure recovery

Status: **COMPLETED**

Completed milestones: **Slices 4.1-4.6 - Reliability policy foundation through resilience verification and operations**

Next milestone: **Phase 5 planning - Durable scheduling and coordination**

Detailed implementation plan: [Phase 4 plan](PHASE_4_PLAN.md)

Completed capabilities:

- Versioned, persisted per-task retry, backoff, timeout, and retry-classification policies
- Durable `RETRY_SCHEDULED` task state and persisted `next_attempt_at` timestamps
- Deterministic exponential backoff with caps and bounded jitter
- Atomic attempt failure, retry decision, state transition, and lifecycle-event persistence
- Horizontally safe due-retry release using bounded `FOR UPDATE SKIP LOCKED` claims
- Retry attempt creation only after the persisted due time
- Retry scheduled, ready, started, and exhausted lifecycle events
- Commit-aware retry counters, backoff distribution, scheduler release, and failure metrics
- Backward-compatible Kafka command/result contracts carrying reliability metadata
- PostgreSQL tests for restart-safe timing, exhaustion, classification, and concurrent schedulers
- Persisted per-attempt deadlines set atomically when work is claimed
- Horizontally safe timeout recovery with bounded `FOR UPDATE SKIP LOCKED` claims
- Timeout recovery routed through the same retry, backoff, exhaustion, and workflow-failure path
- State-version rejection of late results from timed-out or superseded attempts
- Attempt-timeout lifecycle events and commit-aware task, retry, terminal, and workflow metrics
- PostgreSQL tests for deadline boundaries, timeout retries, terminal timeouts, late results, and competing reapers
- Unique per-attempt fencing tokens propagated through commands, heartbeats, results, and completion
- Renewable PostgreSQL worker leases with immediate and periodic versioned heartbeats
- Transactionally idempotent heartbeat ingestion with stale-attempt and stale-token rejection
- Horizontally safe orphan recovery through bounded, workflow-first `FOR UPDATE SKIP LOCKED` claims
- Common retry, backoff, exhaustion, and workflow-failure handling for expired worker leases
- PostgreSQL tests for lease renewal, stale-worker fencing, lease-expiry retry, and competing reapers
- Shared `flowforge-kafka-support` module for consistent consumer recovery semantics across deployables
- Configurable bounded exponential redelivery for command, result, and heartbeat consumers
- Separate versioned command, result, and heartbeat transport DLQ topics
- Broker-acknowledged DLQ publication before source-offset recovery
- Raw payload, source metadata, correlation-header, failure-class, and deterministic source-record identity preservation
- Durable `TASK_DEAD_LETTERED` execution events published through the transactional outbox
- Consumer delivery, recovery, DLQ publication, record-age, exhaustion, and dead-lettered-task metrics
- Live Kafka poison-record tests proving transport separation and continued partition progress
- Replacement-process recovery for pending commands, durable worker results, and offline consumers
- Broker and database publication-failure recovery with stable-ID replay
- Multi-item concurrency tests proving retry schedulers and timeout reapers claim disjoint work
- Retry, outage, worker-loss, and safe DLQ replay operational runbooks
- ADR-004 reliability architecture and failure-ownership decision record

Verification notes:

- The complete Maven reactor succeeds across 44 suites with 128 tests passing and no failures, errors, or skipped tests.
- Rancher Desktop-backed PostgreSQL tests prove concurrent scheduler and timeout-reaper replicas claim disjoint six-item batches and process each of twelve records exactly once.
- Rancher Desktop-backed PostgreSQL and Kafka tests pass all three lease/fencing scenarios, including stale-worker rejection and exactly-once recovery under competing reapers.
- Rancher Desktop-backed Kafka tests route malformed commands, results, and heartbeats to their respective DLQs and continue processing after poison records.
- Recovery tests verify replacement processes, Kafka publish interruption, PostgreSQL acknowledgement interruption, duplicate delivery, and stale results without message loss or duplicate state transitions.

Phase 4 exit criteria are satisfied. Durable schedules, Redis coordination, rate limiting, and backpressure remain Phase 5 scope.

## Phase 5: Scheduling and coordination

Status: **COMPLETED**

Completed milestones: **Slices 5.1-5.6 - Durable scheduling through resilience and operations**

Next milestone: **Phase 6 planning - Observability and horizontal scalability**

Detailed implementation plan: [Phase 5 plan](PHASE_5_PLAN.md)

Completed capabilities:

- Framework-free one-time and cron schedule specifications
- Explicit IANA time-zone and catch-up-once or skip misfire semantics
- Flyway-managed durable schedule definitions and trigger-history foundation
- Create, inspect, list, update, pause, resume, and soft-delete schedule APIs
- Active-workflow and published-version validation
- Strong ETag and `If-Match` optimistic concurrency for schedule mutations
- Durable schedule-trigger history with deterministic logical-fire idempotency keys
- Bounded, multi-replica-safe due-schedule materialization using `FOR UPDATE SKIP LOCKED`
- Token-fenced trigger processing leases with expiry recovery and delayed transient retries
- Exactly-once workflow creation at the durable state boundary despite at-least-once trigger processing
- Catch-up-once and skip misfire policies with a configurable lateness threshold
- Automatic one-time schedule completion and recurring next-fire advancement
- Scheduler lifecycle and failure metrics
- PostgreSQL-authoritative, token-owned coordination permit ledger
- Atomic Redis Lua acquire, renew, release, replacement, expiry cleanup, and active-count operations
- Environment-namespaced, hashed Redis keys with bounded TTLs
- Redis-loss degradation and explicit reconstruction from active PostgreSQL permits
- Redis health checks and bounded coordination metrics
- Versioned per-workflow and per-task concurrency policies in the workflow API and PostgreSQL schema
- PostgreSQL-serialized workflow admission and task dispatch that remains correct through Redis loss
- Durable task deferral at saturation and explicit HTTP 429 workflow-admission responses
- Permit release across completion, retry, cancellation, timeout, and orphan-recovery paths
- Periodic permit renewal, orphan cleanup, expired-token replacement, and Redis reconstruction from execution state
- Bounded concurrency rejection, deferral, permit-lifecycle, and reconciliation metrics
- PostgreSQL-authoritative atomic token buckets for schedule fires and both task-dispatch paths
- Versioned, TTL-bounded Redis rate-limit mirrors reconstructed from durable bucket state
- Hard pending-schedule and per-workflow ready-task queue bounds across replicas
- Retryable HTTP 429 overload responses with `Retry-After` guidance
- Saturation, throttling, queue-depth, queue-age, retry-after, and rejected-admission metrics
- Cooperative sticky Kafka partition rebalancing with explicit graceful-shutdown budgets
- Multi-process recovery from abandoned schedule claims through control-plane and worker replacement
- Redis permit reconstruction during an active scheduled execution without moving correctness out of PostgreSQL
- Phase 5 operations runbook and ADR-005 architecture record
- Domain, cron-calculation, HTTP, and PostgreSQL persistence tests

Verification notes:

- The complete Maven reactor succeeds across 54 suites with 174 tests passing and no failures, errors, or skipped tests.
- Rancher Desktop-backed PostgreSQL tests verify schedule round trips, lifecycle mutations, unpublished-workflow rejection, and stale-version fencing.
- Rancher Desktop-backed PostgreSQL tests verify disjoint competing-scheduler batches, deterministic execution identity, expired-lease takeover, and stale-token fencing.
- Rancher Desktop-backed PostgreSQL and Redis tests verify concurrent capacity enforcement, permit ownership, expiry, key TTLs, full Redis key loss, and state reconstruction.
- Rancher Desktop-backed tests verify concurrent workflow admission, task saturation deferral, retry/timeout/orphan release, expired-token replacement, and Redis reconstruction from active execution state.
- Rancher Desktop-backed tests verify atomic fractional token refill, concurrent consumption without oversubscription, Redis rate-key reconstruction, pending/ready queue bounds, and retryable overload responses.
- Rancher Desktop-backed multi-process tests verify abandoned schedule takeover, permit reconstruction after total Redis key loss, control-plane and worker replacement, offline result recovery, and exactly one durable execution.
- Rancher Desktop-backed Kafka tests verify a group rebalance during an in-flight command preserves exactly one durable completion and converges to disjoint partition ownership.
- Redis remains outside the correctness boundary; PostgreSQL state and resource locks prevent oversubscription even when an ephemeral permit expires or disappears.

Planned scope:

- One-time and recurring workflow schedules
- Durable scheduler claims using PostgreSQL locking
- Redis-backed ephemeral coordination and rate limiting
- Per-workflow and per-task concurrency limits
- Backpressure, admission control, and graceful rebalancing
- Recovery that does not depend on Redis persistence

## Phase 6: Observability and horizontal scalability

Status: **COMPLETED**

Completed milestones: **Slices 6.1-6.6c - Observability, measured scalability, resilience, and operations closeout**

Next milestone: **Phase 7 planning - Security and operational readiness**

Detailed implementation plan: [Phase 6 plan](PHASE_6_PLAN.md)

Completed capabilities:

- Shared observability module with canonical structured-log fields and leak-safe nested MDC scopes
- Bounded caller-supplied HTTP correlation IDs with generated safe fallbacks and response echoing
- Workflow, task, attempt, event, fencing, worker, topic, partition, and offset context across Kafka consumers
- Durable outbox and asynchronous heartbeat publication context across control-plane and worker threads
- Production-only ECS JSON console output through Spring Boot native structured logging
- Context restoration, injection resistance, consumer correlation, and production-profile tests
- Micrometer/OpenTelemetry tracing for HTTP, Kafka, scheduled processing, and worker execution
- Bounded W3C `traceparent`, `tracestate`, and `baggage` persisted across both durable outboxes
- Configurable sampling and opt-in OTLP trace export that remains outside delivery correctness paths
- PostgreSQL/Kafka integration tests proving stable command and result trace propagation
- Prometheus scrape endpoints with service/environment tags and HTTP latency histograms
- Durable workflow throughput counters and current queue, age, schedule, permit, and worker backlog gauges
- Versioned Grafana operations dashboard plus provisioned Prometheus and OpenTelemetry Collector Compose services
- Automated metric semantics, cardinality, dashboard, and provisioning contract tests
- Initial 30-day API availability, workflow-start latency, ready-queue age, and terminal-completion objectives
- Seven-window SLI recording rules with multi-window fast-burn pages and sustained-budget tickets
- Actionable symptom alerts for outboxes, schedules, retries, timeouts, DLQs, permits, admission, dependencies, and rule health
- Versioned SLO/alert runbook, repository contract tests, and pinned `promtool` syntax and behavior validation
- Java 21 paced load generator for API-started and scheduled fan-out/fan-in workflows
- Bounded smoke, overload, soak, and scheduled profiles with thresholded machine-readable JSON reports
- Versioned single-node, two-worker, and balanced two-control-plane/two-worker load topologies
- Deterministic process/container orchestration with health gates, Kafka partition validation, explicit volume-reset safety, and failure-path cleanup
- Combined reports with host/JVM/Docker/Git metadata, Prometheus resource/backlog counters, PostgreSQL probes, Kafka lag/ownership, durable reconciliation, and per-replica useful-work deltas
- Isolated 11-scenario capacity matrix with curated baseline, horizontal, partition/pool-bound, overload, scheduled, and sustained-soak evidence
- Readiness-gated topic provisioning and outbound-producer warm-up that prevent cold-start lease loss and accidental one-partition topic creation
- Priority-aware, bounded-concurrent durable outbox publication with preserved claim fencing and replay semantics
- Measured safe operating envelope, first bottleneck, scaling limits, uncertain-overload behavior, and tuning sequence
- Bounded, auto-restoring PostgreSQL and Kafka fault injection with timestamped, redacted machine-readable evidence
- Retry-capable resilience workloads, dependency-tolerant probes, exact durable reconciliation, and bounded suppressed-redelivery checks
- PostgreSQL-authoritative concurrency evidence through Redis pause, full state flush, and automatic mirror reconstruction
- Telemetry-backend interruption evidence with recovered Prometheus targets and post-restoration OTLP trace export
- Consolidated observability, dashboard, capacity, telemetry, and dependency-recovery operating procedures
- ADR-006 decision record for fail-open telemetry, metric cardinality, durable trace context, and evidence-based scaling
- Required-pass four-scenario resilience matrix with isolated state, deterministic cleanup, redacted evidence, and aggregate reporting

Verification notes:

- The complete Maven reactor succeeds across 68 suites with 211 tests passing and no failures, errors, or skipped tests.
- Request and consumer tests verify unsafe correlation values are replaced and MDC state is cleared or restored after processing.
- Publisher tests verify existing outbox recovery semantics remain intact with scoped structured context.
- The distributed restart test verifies initial tasks cannot bypass the Kafka command path through eager local dispatch.
- A live Rancher Desktop smoke test verifies both Prometheus scrape endpoints, three healthy Prometheus targets, collector startup, and Grafana dashboard provisioning.
- A live Rancher Desktop Prometheus load verifies all 33 recording and alerting rules evaluate healthy.
- A live distributed smoke profile completes 25 of 25 six-task workflows with no errors and records 416.657 ms start p95 and 21.763 second terminal-completion p95.
- Managed single-node smoke evidence completes 25 of 25 workflows and 150 tasks with zero duplicates or scrape errors and exact durable reconciliation.
- Managed two-worker smoke evidence distributes 150 completed commands as 74/76 with zero duplicates; the final balanced two-by-two acceptance run distributes commands as 77/73 and consumed results as 48/102 while all 25 workflows succeed and Kafka ownership remains observable.
- Independent five-second PostgreSQL and Kafka pauses each complete 10 of 10 workflows with zero durable failures; recovery upper bounds are 20 and 22 seconds, and Kafka safely suppresses eight redeliveries.
- Redis pause and state loss complete 10 of 10 workflows with zero permit-limit violations; the ephemeral mirror is reconstructed from PostgreSQL-authoritative permits.
- Simultaneous Prometheus and collector pauses complete 10 of 10 workflows; both scrape targets recover and 11 trace batches export after restoration.
- The required closeout matrix passes all four PostgreSQL, Kafka, Redis, and telemetry scenarios with zero durable workflow failures.
- Repository contract tests cover the final operations runbook, ADR-006 boundaries, resilience manifest, curation, and required-pass semantics.

Phase boundary:

- Phases 1-7 are complete. FlowForge now has a tested distributed runtime plus security, tenancy, secret handling, audit, deployment, delivery automation, logical/physical recovery, rollback compatibility, credential rotation, tenant-safe DLQ replay, and a same-revision release acceptance boundary.

## Phase 7: Security and operational readiness

Status: **COMPLETED**

Completed milestones: **Slices 7.1-7.6.4 - Security and tenancy through operational release closeout**

Next milestone: **Reviewed release-candidate commit and protected publication when authorized**

Detailed implementation plan: [Phase 7 plan](PHASE_7_PLAN.md)

Completed capabilities:

- Stateless OAuth 2.0 JWT bearer authentication with explicit issuer and JWK-set trust configuration
- Deny-by-default HTTP authorization with bounded viewer, operator, administrator, and monitoring roles
- Public health probes, protected metrics, and administrator-only operational endpoints
- Stable authentication/authorization problem responses that do not expose security internals
- Local opt-out for identity-provider-free development and fail-closed production startup requirements
- Strict tenant identifiers derived only from validated JWT claims, with header spoofing rejected by construction
- Leak-safe request/log tenant context and an explicit security-disabled local tenant
- V13 tenant registry and indexed ownership roots for workflows, executions, and schedules
- Populated-schema V12-to-V13 migration verification with backward-compatible ownership backfill
- Tenant-aware workflow aggregate, API, service, and repository contracts
- Explicit workflow ownership writes and tenant predicates across workflow CRUD, publish, and archive operations
- PostgreSQL verification that cross-tenant workflow identifiers return not found and cannot mutate state
- Tenant-aware execution aggregate, API, application service, and repository contracts
- Tenant-scoped execution admission, idempotency replay, lookup, cancellation, and published-workflow validation
- V14 composite ownership constraints preventing executions from referencing workflows owned by another tenant
- PostgreSQL verification that cross-tenant execution identifiers and idempotency keys cannot observe or mutate foreign state
- Tenant-aware message envelopes and exact Kafka tenant-header validation across commands, results, and heartbeats
- V15 control-plane ownership for execution events, inboxes, and outboxes, plus V3 worker ownership for command deduplication and result publication
- PostgreSQL and Kafka verification that result ingestion, duplicate event IDs, worker recovery, and dead-letter records remain isolated by tenant
- Tenant-owned schedules, trigger leases, task queues, concurrency permits, rate buckets, and Redis coordination keyspaces
- V16 schedule and coordination ownership constraints, tenant-scoped capacity locks, and removal of transitional durable-root tenant defaults
- PostgreSQL and Redis verification that schedule admission, lease fencing, coordination capacity, and recovery remain isolated across tenants
- Versioned per-tenant active-execution, running-task, ready-task, pending-schedule, and token-bucket quotas with inherited deployment defaults
- Tenant-context-only quota administration with optimistic ETags and stale-writer fencing
- PostgreSQL-authoritative quota enforcement across replicas, with independent cross-tenant capacity and Redis-loss recovery verification
- Provider-neutral task secret references persisted and transported without resolved material, with worker-only provider resolution
- Recursive inline-secret rejection and worker failure redaction before logs, observations, events, or durable details
- PostgreSQL-enforced append-only administrative audit attempt/outcome pairs with fail-closed mutation admission
- Tenant-scoped administrator audit history and a documented, configurable online retention and immutable archival policy
- Digest-pinned, provenance-enabled, deterministic multi-stage images for independently deployable control-plane and worker runtimes
- Non-root UID/GID 10001 images with container-aware memory policy, application liveness checks, and runtime-only Java 21 layers
- Windows-safe image automation with Git-derived timestamps/OCI metadata, optional registry push, and non-root output validation
- Versioned Helm packaging for independent control-plane/worker scaling, Services, disruption budgets, and optional stabilized HPAs
- Zero-unavailable rollouts with graceful drain windows, startup/liveness/readiness probes, explicit resources, and host/zone topology spread
- Restricted Kubernetes security contexts with read-only roots, dropped capabilities, RuntimeDefault seccomp, and disabled service-account token mounts
- External Secret references for PostgreSQL, Kafka SASL, Redis, OIDC, image pulls, and optional PKCS12 runtime trust material
- Pull-request and main-branch CI gates for fast tests, architecture boundaries, populated-schema upgrades, the complete distributed reactor, Helm/Kubernetes manifests, dependencies, SBOMs, image builds, and fixed HIGH/CRITICAL vulnerabilities
- Cross-platform local quality-gate automation mirroring the CI boundaries
- Manually approved releases with semantic-version validation, bounded distributed smoke acceptance, immutable image digests, registry-attached provenance and SBOMs, and protected-key Cosign signatures and attestations
- Full-commit pinning for every third-party GitHub Action plus executable delivery-pipeline contract tests
- Digest-pinned Ubuntu Noble JRE runtime images and embedded Tomcat 11.0.25 with clean fixed HIGH/CRITICAL Trivy scans
- Transaction-consistent PostgreSQL custom-format backup and isolated transactional-restore proof over populated multi-tenant workflow state
- Recovery-point table-count reconciliation, Flyway validation, tenant/audit invariant verification, and post-snapshot cutoff evidence
- Independent CI/release recovery gate combining PostgreSQL restoration with Redis reconstruction from durable permits and rate buckets
- Conservative recovery runbook defining encrypted backup evidence, clean Redis reconstruction, Kafka recovery-point isolation, abort conditions, and approval records
- Physical PostgreSQL recovery proof with verified base-backup manifests, continuous WAL archival, a named restore point, post-boundary exclusion, and promotion to a new timeline
- Explicit forward-only schema policy backed by a V17-application-on-V18-schema compatibility test
- Restart-driven credential and trust rotation through external Secrets and a non-sensitive Helm pod-template revision
- Provider-specific database, Kafka, Redis, OIDC/JWKS, trust-store, registry, and signing-key rotation procedures
- Administrator-only, tenant-bound single-record DLQ inspection and replay with foreign records hidden as not found
- Payload-free DLQ inspection evidence, fixed source-topic allowlisting, same-partition publication, stable-header preservation, and DLQ-header removal
- Durable PostgreSQL replay claims with idempotency-key and source-location uniqueness, bounded leases, Kafka acknowledgement, and explicit at-least-once ambiguity
- Incident containment, approval, abort, reconciliation, monitoring, and payload-free evidence procedures backed by executable repository contracts
- Independent Security and Upgrade gates mirrored across local automation, pull-request CI, and the protected release workflow
- Fail-fast same-revision release acceptance covering security, upgrade, recovery, the full reactor, deployment manifests, and the aggregate SBOM
- ADR-007 security, tenancy, durable-authority, recovery, artifact-trust, and protected-publication boundaries

Verification notes:

- Security contract tests cover anonymous rejection, role boundaries, public probes, protected metrics, role-claim normalization, and required trust configuration.
- The complete Maven reactor succeeds across 70 suites with 220 tests passing and no failures, errors, or skipped tests.
- After the tenant foundation, the complete reactor succeeds across 73 suites with 228 tests passing and no failures, errors, or skipped tests.
- After workflow-definition tenant isolation, the complete reactor succeeds across 72 suites with 226 tests passing and no failures, errors, or skipped tests.
- After execution ownership and API isolation, the complete reactor succeeds across 72 suites with 227 tests passing and no failures, errors, or skipped tests.
- After event and messaging tenant propagation, the complete reactor succeeds across 73 suites with 232 tests passing and no failures, errors, or skipped tests.
- After schedule and coordination isolation, the complete reactor succeeds across 73 suites with 235 tests passing and no failures, errors, or skipped tests.
- After distributed tenant quotas, the complete reactor succeeds across 76 suites with 244 tests passing and no failures, errors, or skipped tests.
- After secret references, redaction, and security auditing, the complete reactor succeeds across 84 suites with 257 tests passing and no failures, errors, or skipped tests.
- After container and Helm deployment packaging, the complete reactor succeeds across 85 suites with 260 tests passing and no failures, errors, or skipped tests.
- Strict Helm lint, optional-path rendering, and Kubernetes client schema validation pass; both images build on Rancher Desktop and run read-only as UID/GID 10001 on Java 21.0.12.
- After delivery automation and vulnerability remediation, the complete Rancher Desktop-backed reactor succeeds across 86 suites with 264 tests passing and no failures, errors, or skipped tests.
- The final release smoke profile completes 25 of 25 workflows in 26.098 seconds with zero errors, exact durable reconciliation, 438.464 ms start p95, 13.704 second completion p95, and no duplicate worker commands.
- The aggregate CycloneDX 1.6 SBOM contains 156 runtime components and both final images have zero fixed HIGH/CRITICAL Trivy findings.
- The first Slice 7.6 recovery increment restores a populated snapshot into an isolated PostgreSQL database, validates all 19 migrations and every public table count, rejects post-snapshot data, and re-enforces tenant and append-only audit constraints.
- After the first recovery increment, the complete Rancher Desktop-backed reactor succeeds across 87 suites with 266 tests passing and no failures, errors, or skipped tests.
- The second recovery increment restores a verified physical base backup through archived WAL to a named point, promotes to a new timeline, excludes post-boundary data, and certifies the V17 application SQL contract against V18.
- After the second recovery increment, the complete Rancher Desktop-backed reactor succeeds across 89 suites with 268 tests passing and no failures, errors, or skipped tests.
- The third recovery increment proves against real PostgreSQL and Kafka that foreign-tenant DLQ records remain hidden, one owned record is republished to its allowlisted source partition, DLQ-only headers and payload persistence are excluded, and an idempotent retry cannot publish a second record.
- The independent Recovery gate succeeds across 13 logical restore, PITR, compatibility, Redis reconstruction, and DLQ replay acceptance tests.
- After the third recovery increment, the complete Rancher Desktop-backed reactor succeeds across 92 suites with 281 tests passing and no failures, errors, or skipped tests.
- The final same-revision release matrix passes 24 security tests, two upgrade/rollback tests, thirteen recovery tests, strict Helm/kubectl validation, and generation of a validated 156-component CycloneDX 1.6 SBOM.
- After the release-closeout contract, the complete clean Rancher Desktop-backed reactor succeeds across 92 suites with 282 tests passing and no failures, errors, or skipped tests.
