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

Next: **Slice 6.3 - Prometheus metrics and Grafana dashboards**

### Slice 6.2: OpenTelemetry tracing and propagation

Status: **COMPLETED**

- Add Micrometer tracing with OpenTelemetry/OTLP export and bounded, configurable sampling.
- Instrument HTTP, Kafka send/consume, schedule materialization, workflow transitions, and worker handlers.
- Propagate W3C `traceparent` and `baggage` through Kafka without replacing durable event or correlation IDs.
- Record errors and state outcomes without attaching payloads or unbounded attributes.
- Verify context propagation and safe operation when the collector is absent.

Exit: one scheduled workflow can be followed across API or scheduler, outbox, Kafka, worker, result ingestion, and DAG advancement in a trace backend, with telemetry disabled or unavailable remaining non-blocking.

Implemented checkpoint:

- Added Spring Boot's Micrometer/OpenTelemetry integration to the control plane and worker, with configurable probability sampling, W3C propagation, bounded attributes, and opt-in OTLP trace export.
- Enabled Spring Kafka observations for producers and consumers and added explicit observations around schedule processing and worker task execution with low-cardinality outcome tags and error recording.
- Added a bounded shared trace-context carrier and persisted `traceparent`, `tracestate`, and `baggage` alongside both control-plane and worker outbox records.
- Restored the initiating context when a leased outbox record is published so traces survive database commits, publisher delays, process replacement, and result publication without coupling telemetry to delivery correctness.
- Added Flyway migrations with length and control-character constraints for both durable outboxes.
- Verified W3C context capture/restoration and end-to-end Kafka command/result propagation against real PostgreSQL and Kafka containers, including stable trace IDs and baggage across durable boundaries.
- Kept trace export disabled by default; unavailable exporters remain outside workflow transactions and acknowledgement paths.
- Removed a distributed-mode eager-dispatch race exposed by the recovery suite so initial tasks deterministically remain on the durable Kafka command path.
- Verified the complete Maven reactor: 187 tests across 60 suites, with no failures, errors, or skipped tests.

Next: **Slice 6.3 - Prometheus metrics and Grafana dashboards**

### Slice 6.3: Prometheus metrics and Grafana dashboards

Status: **COMPLETED**

- Add the Prometheus registry and production scrape endpoint.
- Normalize timers, counters, gauges, descriptions, and service/common tags.
- Add versioned Grafana dashboards for workflow throughput, queue health, Kafka/outbox flow, retries/DLQs, scheduling, concurrency, and dependency health.
- Add a local Prometheus/Grafana/collector profile to Compose.
- Test scrape availability and validate dashboard metric references.

Exit: a local distributed run exposes actionable RED and queue/saturation views without high-cardinality metric labels.

Implemented checkpoint:

- Added Prometheus registries and scrape endpoints to both independently deployable applications with stable application and environment tags.
- Added PostgreSQL-backed monotonic workflow counters plus current execution, task, outbox, schedule, permit, worker-command, and worker-result backlog gauges.
- Made durable-state metric callbacks return `NaN` during dependency loss so a failed telemetry read cannot affect workflow processing or fail the entire scrape.
- Enabled HTTP latency histograms and retained Spring Kafka, JVM, process, retry, timeout, DLQ, admission, coordination, and publisher metrics.
- Added a versioned twelve-panel Grafana dashboard covering RED signals, throughput, queue depth and age, reliability, scheduling, concurrency, Redis failures, JVM heap, and CPU.
- Added a Compose `observability` profile with pinned Prometheus, Grafana, and OpenTelemetry Collector images plus repository-managed provisioning.
- Added automated metric semantics, low-cardinality, dashboard JSON, provisioning, and scrape-target contract tests.
- Verified a live Rancher Desktop run in which Prometheus reported the control plane, worker, and itself healthy and Grafana loaded the provisioned dashboard.
- Verified the complete Maven reactor: 192 tests across 63 suites, with no failures, errors, or skipped tests.

Next: **Slice 6.4 - SLOs and actionable alerts**

### Slice 6.4: SLOs and actionable alerts

Status: **COMPLETED**

- Define service-level indicators for API availability, workflow start latency, task queue latency, and terminal completion.
- Add Prometheus recording and alerting rules for backlog age, outbox stalls, retry/timeout spikes, permit leaks, admission saturation, DLQ traffic, and dependency loss.
- Use multi-window burn-rate alerts where an SLO applies and symptom-based alerts elsewhere.
- Add alert annotations that link directly to repository runbooks.
- Validate rule syntax and metric references automatically.

Exit: alerts identify user-visible risk early, avoid entity-ID cardinality, and include a concrete first response.

Implemented:

- Defined initial 30-day objectives for API availability, successful workflow-start latency, durable ready-queue age, and active-execution completion age, including explicit event/sample semantics and exclusions.
- Added seven-window recording rules and multi-window, multi-burn-rate page/ticket alerts for the request-based availability and workflow-start SLOs.
- Added state-based alerts for queue age, terminal completion, both transactional outboxes, schedule backlog, retry exhaustion, timeouts, dead-letter traffic, permit leaks, admission saturation, Redis coordination failures, scrape loss, and Prometheus evaluation failures.
- Added an active-execution oldest-age gauge backed by PostgreSQL and retained only bounded deployment dimensions in all SLI records and alerts.
- Added a versioned alert-response runbook with a concrete first response for each SLO and direct links from every alert annotation to the relevant Phase 4, 5, or 6 procedure.
- Added repository contract tests for SLO/rule coverage, emitted and recorded metric references, low-cardinality policy, Compose mounting, and local runbook targets.
- Added a repeatable PowerShell validator using the pinned Prometheus image plus `promtool` behavior tests for fast-burn and persistence-threshold alert semantics.
- Verified all 33 rules load and evaluate healthy in the Compose Prometheus service on Rancher Desktop.
- Verified the complete Maven reactor: 193 tests across 63 suites, with no failures, errors, or skipped tests.

Next: **Slice 6.5 - Load, partitioning, and capacity model**

### Slice 6.5: Load, partitioning, and capacity model

Status: **IN PROGRESS**

- Build a reproducible workload generator for API-started and scheduled fan-out/fan-in workflows.
- Measure one and multiple control-plane/worker replicas across Kafka partition counts and database pool sizes.
- Capture throughput, p50/p95/p99 latency, queue age, retries, duplicates, errors, CPU, memory, database locks, and consumer lag.
- Add bounded overload and soak profiles with machine-readable summaries and threshold assertions.
- Document the Kafka partition, worker concurrency, PostgreSQL, Redis, and rate-limit capacity model.

Exit: checked-in commands and reports demonstrate horizontal work distribution, the first bottleneck, and a defensible tuning sequence.

#### Slice 6.5a: Reproducible workload and threshold harness

Status: **COMPLETED**

- Added an independently packaged Java 21 load-test module using paced arrivals, virtual threads, and a hard in-flight bound.
- Generate a real fan-out/fan-in workflow through the public API, publish it with optimistic concurrency, and use a unique idempotency key for every API-started execution.
- Support both API-started execution/terminal polling and one-time schedule/fire-lag workloads.
- Emit versioned JSON reports with configuration, counts, HTTP statuses, throughput, p50/p95/p99/max start and completion latency, and explicit threshold results.
- Added bounded smoke, overload, soak, and scheduled profiles plus a PowerShell entry point that fails automation when a threshold fails.
- Added validation, percentile, and concurrent HTTP contract tests.
- Verified the packaged executable against real local PostgreSQL, Kafka, Redis, control-plane, and worker processes: 25 of 25 six-task workflows completed, with no rejection, timeout, terminal failure, or transport error; start p95 was 416.657 ms and completion p95 was 21.763 seconds.
- Verified the complete Maven reactor: 198 tests across 66 suites, with no failures, errors, or skipped tests.

Next: **Slice 6.5c - Measured matrix and capacity model**

#### Slice 6.5b: Topology orchestration and resource probes

Status: **COMPLETED**

- Start and stop named one/multi-replica topologies with explicit ports, instance IDs, database pools, worker concurrency, and Kafka partition settings.
- Record host/JVM metadata, process CPU and memory, PostgreSQL connections/locks, Kafka partition ownership and consumer lag, queue age, retries, duplicates, and errors alongside each workload report.
- Keep process lifecycle cleanup deterministic after successful and failed runs.
- Added versioned single-node, two-worker, and balanced two-control-plane/two-worker topologies with explicit six-partition Kafka configuration.
- Added a safe managed runner that validates topology and topic contracts, waits for health, round-robins API load across control planes, captures a combined JSON evidence report, and cleans up owned processes on success or failure. Volume reset remains explicitly opt-in.
- Added per-replica Prometheus sampling, PostgreSQL connection/lock probes, Kafka group lag and partition-ownership snapshots, durable execution reconciliation, and worker/control-plane useful-work deltas.
- Verified the single-node topology with 25 of 25 workflows and 150 of 150 task results, zero duplicates, zero scrape errors, and matching durable counts.
- Verified two workers shared 150 commands as 74/76 with zero duplicates; the final balanced acceptance run shared commands as 77/73 and consumed results as 48/102 while all 25 workflows completed durably, and captured 54 partition-assignment rows across three samples.
- Verified the complete Maven reactor: 201 tests across 67 suites, with no failures, errors, or skipped tests.

Next: **Slice 6.5c - Measured matrix and capacity model**

#### Slice 6.5c: Measured matrix and capacity model

Status: **COMPLETED**

- Run bounded baseline, horizontal-scale, partition-bound, pool-bound, overload, scheduled, and soak scenarios.
- Check in curated machine-readable reports with topology and environment metadata.
- Document the observed first bottleneck, safe operating envelope, scaling limits, and tuning sequence without generalizing laptop results into universal capacity claims.
- Added isolated reset-per-scenario execution, readiness-gated Kafka topic provisioning, producer warm-up, bounded concurrent outbox publication, and explicit unexpected-response accounting after the matrix exposed cold-start and queueing races.
- Checked in 11 curated scenario reports plus an aggregate summary; baseline, balanced-horizontal, scheduled, and sustained-soak safe-envelope gates pass.
- Recorded the six-partition consumer ceiling, PostgreSQL pool/lock pressure, worker-lease expiry boundary, uncertain overload outcomes, and evidence-based tuning sequence in `docs/performance/CAPACITY_MODEL.md`.
- Verified the complete Maven reactor after the matrix-driven fixes: 205 tests across 68 suites, with no failures, errors, or skipped tests.

Next: **Slice 6.6 - Resilience validation and operations closeout**

### Slice 6.6: Resilience validation and operations closeout

Status: **COMPLETED**

- Inject PostgreSQL, Kafka, Redis, and telemetry-backend latency/outages during load.
- Verify recovery time, bounded queues, duplicate suppression, fencing, and telemetry continuity/degradation.
- Add observability, SLO, dashboard, and capacity runbooks.
- Record the telemetry and cardinality decisions in ADR-006.
- Run the complete Maven suite and the documented bounded resilience scenario.

Exit: Phase 6 evidence shows that multiple replicas remain correct and observable through overload and dependency recovery, and operators have versioned diagnostics for every alert.

#### Slice 6.6a: PostgreSQL and Kafka disruption recovery

Status: **COMPLETED**

- Add versioned, bounded fault plans to the managed topology runner with exact start/restore evidence and unconditional dependency restoration.
- Keep already accepted load operations polling through transient transport failures and report those failures separately from terminal operation errors.
- Pause PostgreSQL and Kafka independently only after workload admission, then assert durable completion, duplicate suppression, bounded backlog, and measured recovery time.

Implemented:

- Added versioned PostgreSQL and Kafka pause plans, a bounded hidden fault injector, exact UTC fault/restoration evidence, workload-overlap checks, and idempotent cleanup that restores dependencies after success or failure.
- Made PostgreSQL and Kafka probes degrade to explicit unavailable evidence instead of terminating workload execution, and kept already accepted operations polling through transient HTTP transport failures.
- Added explicit retry policy controls to the generated resilience workflow so broker outages can exercise lease-expiry recovery and fencing rather than relying on an outage shorter than the worker lease.
- Verified 10 of 10 workflows through independent five-second PostgreSQL and Kafka pauses with zero durable failures. PostgreSQL recovered within a 20-second workload-level upper bound; Kafka recovered within 22 seconds and safely suppressed eight at-least-once redeliveries.
- Checked in redacted complete reports, an aggregate summary, reproduction commands, measurement semantics, and repository contract tests.
- Verified the complete Maven reactor: 208 tests across 68 suites, with no failures, errors, or skipped tests.

Next: **Slice 6.6b - Redis and telemetry degradation**

#### Slice 6.6b: Redis and telemetry degradation

Status: **COMPLETED**

- Remove and pause Redis state during load, verify PostgreSQL-authoritative capacity remains fenced, and prove mirror reconstruction.
- Interrupt Prometheus and the OpenTelemetry Collector while workflows continue, then verify scrape/export recovery without delivery impact.

Implemented:

- Added explicit workflow/task concurrency limits to resilience traffic so Redis disruption exercises real ephemeral permit mirrors while PostgreSQL remains the durable fencing authority.
- Added bounded Redis pause and state-flush injection, exact key reconstruction evidence, Redis failure/reconciliation counters, and sampled PostgreSQL permit-limit violation checks.
- Added observability-profile lifecycle management and simultaneous Prometheus/collector pauses with recovered readiness, healthy scrape targets, collector connectivity, and post-restoration trace-export assertions.
- Verified 10 of 10 workflows through Redis and telemetry disruptions with zero durable failures and zero permit-limit violations. Redis recorded 16 degraded operations and reconstructed nine keys; telemetry recovered both scrape targets and exported 11 trace batches.
- Checked in complete redacted reports and extended the four-scenario resilience summary and reproduction guide.
- Verified the complete Maven reactor: 209 tests across 68 suites, with no failures, errors, or skipped tests.

Next: **Slice 6.6c - Operations and architecture closeout**

#### Slice 6.6c: Operations and architecture closeout

Status: **COMPLETED**

- Add observability, SLO, dashboard, capacity, and dependency-failure operating procedures.
- Record telemetry boundaries, cardinality policy, and failure semantics in ADR-006.
- Run the complete Maven suite and curated bounded resilience matrix, then close Phase 6 with checked-in evidence.

Implemented:

- Added a consolidated operations runbook covering local stack validation, dashboard triage, telemetry degradation, capacity/overload response, dependency recovery, resilience reproduction, evidence curation, and incident closure.
- Accepted ADR-006: bounded metric cardinality, durable W3C trace propagation, fail-open asynchronous telemetry, versioned SLO/dashboard assets, and evidence-based scaling boundaries.
- Added a versioned four-scenario resilience manifest and required-pass matrix runner with isolated state, deterministic cleanup, automatic host redaction, curated reports, and aggregate generation.
- Ran the closeout matrix on Rancher Desktop. PostgreSQL, Kafka, Redis state loss, and simultaneous Prometheus/collector interruption each completed 10 of 10 workflows with zero durable failures; Redis recorded zero permit-limit violations and telemetry recovered two scrape targets plus 11 trace batches.
- Verified the complete Maven reactor: 211 tests across 68 suites, with no failures, errors, or skipped tests.

Phase 6 exit: **SATISFIED**

Next: **Phase 7 - Security and operational readiness**

## Deliberately deferred

- Authentication, tenant isolation and quotas, secret-reference handling, deployment manifests, delivery automation, and backup/restore remain Phase 7.
- Production capacity numbers for hardware not represented by the checked-in environment are explicitly out of scope.

## Test strategy

- Unit tests for logging/tracing context lifecycle, tag policies, and metric semantics.
- HTTP and Kafka integration tests for correlation and W3C propagation.
- Repository validation for dashboards, Prometheus rules, and runbook links.
- Testcontainers-based multi-replica correctness and dependency-failure tests.
- Separate bounded load profiles so normal unit/integration verification remains deterministic.
