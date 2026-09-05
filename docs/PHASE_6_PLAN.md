# Phase 6 plan: observability and horizontal scalability

## Objective

Make FlowForge diagnosable across API, scheduler, Kafka, control-plane, worker, PostgreSQL, and Redis boundaries, then validate and document how it scales under sustained load and dependency failure.

Observability must preserve the platform's existing correctness model. Telemetry export is best effort, bounded, and never part of a workflow transaction or Kafka acknowledgement decision.

## Engineering invariants

- Every request and message-processing log can be correlated without logging payloads or secrets.
- Workflow, task, attempt, event, fencing, schedule, and trigger identifiers use consistent low-cardinality field names where applicable; metrics never use entity IDs as tags.
- W3C trace context crosses HTTP and Kafka boundaries while durable business correlation remains independent of trace sampling.
- A missing or unavailable telemetry backend cannot block workflow progress.
- Dashboards and alerts are versioned, reviewable repository assets backed by emitted metric names.
- Load results state the topology, partition count, data volume, latency percentiles, throughput, error rate, and resource saturation; a laptop benchmark is not presented as a universal capacity claim.
- Horizontal tests prove useful work distribution and correctness, not merely that multiple processes start.

## Incremental implementation

### Slice 6.1: Correlation-aware structured logging

Status: **COMPLETED**

- Add a shared, leak-safe logging context for request and Kafka consumer threads.
- Assign or accept a bounded HTTP correlation ID and return it to callers.
- Enrich command, result, and heartbeat handling with stable workflow, task, attempt, event, topic, partition, and offset fields.
- Enable ECS JSON console logs in production profiles while retaining readable development/test logs.
- Test context restoration, invalid inbound identifiers, and consumer-thread cleanup.

Exit: API and Kafka processing logs contain consistent correlation fields, untrusted header values cannot inject log content, and pooled threads do not retain a previous operation's context.

Implemented checkpoint:

- Added a framework-light `flowforge-observability` module with canonical log-field names, bounded correlation-ID policy, and nested MDC scopes that restore the complete previous thread context.
- Added the `X-Correlation-Id` HTTP boundary: safe caller values are echoed, while missing, oversized, or control-character values receive a generated UUID.
- Enriched command, result, and heartbeat consumers plus control-plane and worker outbox publishers with correlation, durable entity, attempt, fencing, and Kafka-position fields.
- Re-established context inside asynchronous heartbeat publication callbacks and verified that request, listener, scheduler, and callback threads do not retain prior identifiers.
- Enabled Spring Boot's native ECS structured console format only in production profiles through `FLOWFORGE_LOG_FORMAT`.
- Added focused policy, nesting, filter, consumer, and production-profile tests.
- Verified the complete Maven reactor: 182 tests across 59 suites, with no failures, errors, or skipped tests.

Next: **Slice 6.2 - OpenTelemetry tracing and propagation**

### Slice 6.2: OpenTelemetry tracing and propagation

Status: **PLANNED**

- Add Micrometer tracing with OpenTelemetry/OTLP export and bounded, configurable sampling.
- Instrument HTTP, Kafka send/consume, schedule materialization, workflow transitions, and worker handlers.
- Propagate W3C `traceparent` and `baggage` through Kafka without replacing durable event or correlation IDs.
- Record errors and state outcomes without attaching payloads or unbounded attributes.
- Verify context propagation and safe operation when the collector is absent.

Exit: one scheduled workflow can be followed across API or scheduler, outbox, Kafka, worker, result ingestion, and DAG advancement in a trace backend, with telemetry disabled or unavailable remaining non-blocking.

### Slice 6.3: Prometheus metrics and Grafana dashboards

Status: **PLANNED**

- Add the Prometheus registry and production scrape endpoint.
- Normalize timers, counters, gauges, descriptions, and service/common tags.
- Add versioned Grafana dashboards for workflow throughput, queue health, Kafka/outbox flow, retries/DLQs, scheduling, concurrency, and dependency health.
- Add a local Prometheus/Grafana/collector profile to Compose.
- Test scrape availability and validate dashboard metric references.

Exit: a local distributed run exposes actionable RED and queue/saturation views without high-cardinality metric labels.

### Slice 6.4: SLOs and actionable alerts

Status: **PLANNED**

- Define service-level indicators for API availability, workflow start latency, task queue latency, and terminal completion.
- Add Prometheus recording and alerting rules for backlog age, outbox stalls, retry/timeout spikes, permit leaks, admission saturation, DLQ traffic, and dependency loss.
- Use multi-window burn-rate alerts where an SLO applies and symptom-based alerts elsewhere.
- Add alert annotations that link directly to repository runbooks.
- Validate rule syntax and metric references automatically.

Exit: alerts identify user-visible risk early, avoid entity-ID cardinality, and include a concrete first response.

### Slice 6.5: Load, partitioning, and capacity model

Status: **PLANNED**

- Build a reproducible workload generator for API-started and scheduled fan-out/fan-in workflows.
- Measure one and multiple control-plane/worker replicas across Kafka partition counts and database pool sizes.
- Capture throughput, p50/p95/p99 latency, queue age, retries, duplicates, errors, CPU, memory, database locks, and consumer lag.
- Add bounded overload and soak profiles with machine-readable summaries and threshold assertions.
- Document the Kafka partition, worker concurrency, PostgreSQL, Redis, and rate-limit capacity model.

Exit: checked-in commands and reports demonstrate horizontal work distribution, the first bottleneck, and a defensible tuning sequence.

### Slice 6.6: Resilience validation and operations closeout

Status: **PLANNED**

- Inject PostgreSQL, Kafka, Redis, and telemetry-backend latency/outages during load.
- Verify recovery time, bounded queues, duplicate suppression, fencing, and telemetry continuity/degradation.
- Add observability, SLO, dashboard, and capacity runbooks.
- Record the telemetry and cardinality decisions in ADR-006.
- Run the complete Maven suite and the documented bounded resilience scenario.

Exit: Phase 6 evidence shows that multiple replicas remain correct and observable through overload and dependency recovery, and operators have versioned diagnostics for every alert.

## Deliberately deferred

- Authentication, tenant isolation and quotas, secret-reference handling, deployment manifests, delivery automation, and backup/restore remain Phase 7.
- Production capacity numbers for hardware not represented by the checked-in environment are explicitly out of scope.

## Test strategy

- Unit tests for logging/tracing context lifecycle, tag policies, and metric semantics.
- HTTP and Kafka integration tests for correlation and W3C propagation.
- Repository validation for dashboards, Prometheus rules, and runbook links.
- Testcontainers-based multi-replica correctness and dependency-failure tests.
- Separate bounded load profiles so normal unit/integration verification remains deterministic.
