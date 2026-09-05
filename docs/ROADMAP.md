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
| Phase 5 | PLANNED | Durable scheduling, Redis coordination, and backpressure |
| Phase 6 | PLANNED | Observability, scalability validation, and resilience testing |
| Phase 7 | PLANNED | Security, delivery automation, and operational readiness |

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

Status: **PLANNED**

Planned scope:

- One-time and recurring workflow schedules
- Durable scheduler claims using PostgreSQL locking
- Redis-backed ephemeral coordination and rate limiting
- Per-workflow and per-task concurrency limits
- Backpressure, admission control, and graceful rebalancing
- Recovery that does not depend on Redis persistence

## Phase 6: Observability and horizontal scalability

Status: **PLANNED**

Planned scope:

- Structured logs with workflow, task, attempt, and correlation identifiers
- OpenTelemetry traces across API, Kafka, orchestrator, scheduler, and worker boundaries
- Micrometer metrics and Prometheus/Grafana dashboards
- Queue-latency, retry, timeout, saturation, and DLQ alerts
- Multi-instance load and soak testing
- Documented partitioning and capacity model
- Resilience tests for dependency and infrastructure outages

## Phase 7: Security and operational readiness

Status: **PLANNED**

Planned scope:

- Authentication and role-based authorization
- Namespace or tenant isolation
- Secret-reference handling and sensitive-data redaction
- Audit logging and retention policies
- Container images and deployment manifests
- CI/CD quality and security gates
- Backup, restore, upgrade, incident, and DLQ runbooks
