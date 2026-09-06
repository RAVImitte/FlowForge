# ADR-006: Fail-open telemetry and evidence-based horizontal scalability

## Status

Accepted

## Context

FlowForge spans HTTP, schedulers, PostgreSQL transactions, transactional outboxes, Kafka consumers, worker execution, Redis mirrors, retry/timeout recovery, and multiple independently scalable processes. Operators need to follow one workflow across those boundaries and detect user-visible risk without turning telemetry into another correctness dependency.

Unbounded metric labels would make Prometheus unstable. In-memory trace context would disappear across outbox delay or process replacement. Synchronous export could block a transaction or acknowledgement. Replica counts alone do not prove useful horizontal scaling, and a laptop throughput number is not a production capacity promise.

## Decision

### Correlation and structured logs

- `X-Correlation-Id` is accepted only when bounded and free of control characters; otherwise FlowForge generates a UUID.
- Canonical structured fields cover correlation, workflow, execution, task, attempt, event, worker, fencing, topic, partition, and offset identifiers.
- MDC scopes restore the previous complete context and are closed at HTTP, listener, scheduler, publisher, and asynchronous callback boundaries.
- Production uses ECS JSON console logs. Payloads, secrets, and unbounded task configuration are not logging context.

### Trace continuity

- W3C `traceparent`, `tracestate`, and bounded `baggage` are the propagation format.
- Trace context is persisted with control-plane and worker outbox rows and restored when a leased record publishes. This preserves causal continuity across transaction commit, publisher delay, process replacement, and Kafka transport.
- Durable correlation and event IDs remain the diagnostic authority when a trace is unsampled or export fails.
- Sampling is configurable and OTLP export is opt-in. Export is asynchronous, bounded, and never part of a database transaction, Kafka acknowledgement, lease, fencing, idempotency, or terminal-state decision.

### Metric cardinality

- Metrics use bounded deployment and operation dimensions such as application, environment, route, outcome, scope, topic, and operation.
- Workflow, execution, task, attempt, event, correlation, trace, idempotency, worker-instance, and fencing identifiers are prohibited as metric labels.
- Entity-level diagnosis moves from a bounded metric to a trace or structured log, then to durable PostgreSQL state.
- Durable gauges return unavailable/`NaN` on observation failure rather than failing the scrape or workflow path.

### SLOs, dashboards, and alerts

- Initial SLOs cover API availability, successful workflow-start latency, oldest ready-task age, and oldest active-execution age.
- Request SLIs use paired multi-window burn alerts; sparse durable-state age objectives and infrastructure risks use persistent symptom alerts.
- Every alert carries severity, action, and a repository runbook link. Dashboard, scrape, recording-rule, alert-rule, and provisioning assets are versioned and contract-tested.
- Metric and alert thresholds are starting policy and must be recalibrated using representative workload evidence.

### Capacity evidence

- Load reports record workload shape, topology, Kafka partitions and ownership, useful work per replica, latency percentiles, throughput, errors, retries, duplicates, queue age, consumer lag, JVM use, PostgreSQL connections/locks, and durable outcomes.
- Scenario comparison is valid only for identical workload profiles with isolated state.
- Active command consumers are bounded by Kafka partitions. Worker and control-plane replicas are considered useful only when completed work distribution and end-to-end outcomes improve.
- PostgreSQL contention, result ingestion, leases, and queue age are evaluated before adding replicas. Local measurements establish a checked operating point, not a universal capacity claim.

### Failure semantics

| Failure | Telemetry behavior | Workflow behavior |
|---|---|---|
| Prometheus or Grafana unavailable | Central metrics/dashboard are stale or absent | Processing continues; direct health and scrape endpoints remain available |
| Collector/exporter unavailable | Spans may queue within bounds or be dropped | Transactions, delivery, and acknowledgements continue |
| Redis unavailable or empty | Degradation/reconciliation metrics may be incomplete during loss | PostgreSQL-authoritative permits and buckets preserve fencing; mirrors rebuild |
| PostgreSQL unavailable | Durable gauges may be `NaN`; traces/logs show dependency error | No state transition commits; work recovers after database restoration |
| Kafka unavailable | Lag observation may fail; outbox age rises | Stable outbox identities publish after broker recovery |
| Process exits | In-process spans/metrics may be lost | Durable leases, outboxes, inboxes, and fencing enable replacement recovery |

## Consequences

FlowForge remains correct when every telemetry backend is unavailable, while durable trace propagation and structured IDs make sampled executions diagnosable across process boundaries. Prometheus stays bounded because entity IDs are excluded from labels. Operators can distinguish a telemetry outage from a delivery outage and have reproducible evidence for scaling and recovery claims.

The design accepts incomplete traces, delayed scrapes, approximate sampled-state SLIs, and extra persistence bytes for trace carriers. PostgreSQL-backed metrics add diagnostic queries and can be unavailable during a database outage. Maintaining dashboards, rules, runbooks, workload profiles, and curated evidence is ongoing engineering work.

The [Phase 6 operations runbook](../operations/phase-6-observability-capacity-resilience-runbook.md) defines diagnosis and recovery. SLO policy is detailed in the [SLO and alerting runbook](../operations/phase-6-slo-alerting-runbook.md), and measured limits are documented in the [capacity model](../performance/CAPACITY_MODEL.md). Durable delivery and coordination correctness remain governed by [ADR-004](004-phase-4-reliability-and-recovery.md) and [ADR-005](005-phase-5-scheduling-and-coordination.md).
