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
| Phase 3 | PLANNED | Kafka messaging, transactional outbox, and distributed workers |
| Phase 4 | PLANNED | Retries, timeouts, dead-letter queues, and failure recovery |
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

Status: **PLANNED**

Planned scope:

- Kafka task-command and lifecycle-event topics
- Transactional outbox for PostgreSQL-to-Kafka delivery
- Consumer inbox and event-ID deduplication
- Independently deployable worker process
- Kafka consumer-group worker coordination
- Versioned event contracts and compatibility tests
- At-least-once delivery with idempotent processing

## Phase 4: Reliability and failure recovery

Status: **PLANNED**

Planned scope:

- Configurable retry policies with exponential backoff and jitter
- Durable delayed retries and task deadlines
- Task and workflow timeouts
- Worker heartbeats, leases, fencing tokens, and stale-result rejection
- Poison-message and exhausted-task dead-letter queues
- Orphaned-work recovery after worker or orchestrator failure
- Failure-injection and duplicate-delivery tests

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
