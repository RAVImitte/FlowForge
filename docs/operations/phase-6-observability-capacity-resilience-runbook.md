# Phase 6 observability, capacity, and resilience runbook

## Scope and safety

This runbook covers local and production-like diagnosis of FlowForge metrics, dashboards, traces, structured logs, saturation, and dependency recovery. PostgreSQL is the durable correctness boundary. Kafka transports stable event identities, and Redis is a reconstructable coordination mirror. Prometheus, Grafana, and OpenTelemetry are diagnostic dependencies only.

Never repair an incident by editing workflow, task, attempt, outbox, inbox, permit, rate-bucket, or fencing rows; deleting Kafka offsets; or manufacturing Redis keys. Preserve evidence, restore the failed dependency, and let the durable recovery paths converge.

The checked-in capacity and recovery values describe the recorded Rancher Desktop host and workload. They are not production limits or recovery commitments.

## Start and validate the local stack

From the repository root:

```powershell
docker compose --profile observability config --quiet
docker compose --profile observability up -d postgres kafka redis prometheus grafana otel-collector
docker compose --profile observability ps
Invoke-RestMethod http://localhost:8080/actuator/health/readiness
Invoke-RestMethod http://localhost:8081/actuator/health/readiness
Invoke-RestMethod http://localhost:9090/-/ready
Invoke-RestMethod http://localhost:9090/api/v1/targets
```

Prometheus is at `http://localhost:9090`, Grafana at `http://localhost:3000`, the control-plane scrape at `http://localhost:8080/actuator/prometheus`, and the worker scrape at `http://localhost:8081/actuator/prometheus`. The provisioned dashboard UID is `flowforge-overview`.

The Prometheus container must reach the host JVMs through `host.docker.internal`. On Rancher Desktop, do not add a Docker `host-gateway` override; it can select a different gateway from the one Rancher exposes.

## First-response diagnostic flow

1. Record the alert name, environment, first firing time, affected workflow IDs, and correlation or trace IDs. Do not copy task payloads into incident notes.
2. Check application readiness and Prometheus target health. A missing scrape is not proof that workflow processing stopped.
3. Check durable queue age before queue depth: ready tasks, both outboxes, schedules, and oldest active execution.
4. Compare Kafka lag and partition ownership with worker processing and result-consumer rates.
5. Check PostgreSQL pool pressure and blocked locks before adding replicas.
6. Check Redis failure and permit-reconciliation counters, remembering that Redis loss must not change durable admission correctness.
7. Correlate the first abnormal metric with structured logs and a sampled trace. Durable IDs remain available when a trace was not sampled.
8. Mitigate intake or restore the dependency, then confirm durable queues drain and terminal counts converge.

## Dashboard interpretation

Use the FlowForge overview dashboard in this order:

| Signal | Question | Escalation evidence |
|---|---|---|
| HTTP rate/errors/latency | Can callers start and inspect work? | Sustained `5xx`, start-latency burn, or admission rejection |
| Active executions and oldest age | Is end-to-end completion progressing? | Oldest active age rises across two scrape intervals |
| Ready queue depth and age | Can the control plane dispatch runnable tasks? | Age rises while worker capacity is idle |
| Control-plane outbox | Can committed commands reach Kafka? | Pending count and oldest age rise together |
| Worker processing/result outbox | Can workers execute and publish results? | Processing stalls or result age rises |
| Kafka/retry/timeout/DLQ | Is delivery recovering safely? | Lag persists, retries exhaust, or DLQ counters increase |
| Permits/admission/Redis | Is coordination saturated or degraded? | Permit count diverges from work, admission saturates, or Redis failures rise |
| JVM heap and CPU | Is an instance resource-bound? | Sustained saturation aligned with queue-age growth |

Entity IDs are intentionally absent from metric labels. Use bounded metric dimensions to locate the affected service/environment, then use correlation IDs, trace IDs, and durable database identifiers for a single workflow.

## Telemetry degradation

Telemetry export is fail-open and outside workflow transactions, Kafka acknowledgement, leases, fencing, and idempotency decisions.

If Prometheus is unavailable:

1. Query each application's readiness and `/actuator/prometheus` endpoint directly.
2. Inspect `docker compose --profile observability logs prometheus` and validate the checked-in rules.
3. Restore Prometheus and confirm both FlowForge targets report `health: up` in `/api/v1/targets`.
4. Confirm rule evaluation is healthy before closing the incident; a green application target does not prove alert rules are valid.

If the OpenTelemetry Collector is unavailable:

1. Continue diagnosis from durable IDs, ECS logs, and metrics. Missing spans must not be treated as missing workflow transitions.
2. Check port `4318`, collector logs, exporter errors, and configured `FLOWFORGE_OTLP_TRACES_ENDPOINT`.
3. Restore the collector and confirm new trace batches arrive. Do not restart healthy FlowForge instances merely to recreate spans that were dropped.
4. Reduce sampling if exporter pressure affects process resources; do not make export synchronous with business processing.

Validate rule syntax with the pinned image:

```powershell
.\scripts\verify-observability.ps1
docker compose --profile observability config --quiet
```

Detailed SLO semantics, burn windows, and alert-specific responses are in the [SLO and alerting runbook](phase-6-slo-alerting-runbook.md).

## Capacity and overload response

The scaling order is partitions, useful worker consumers, database pools/locks, result ingestion, and only then additional control-plane replicas. More replicas can increase shared-row contention.

When queue age or completion latency rises:

1. Identify the offered workflow shape and arrival rate; do not compare different profiles as if they were the same workload.
2. Confirm active command consumers do not exceed `min(Kafka partitions, worker replicas * listener concurrency)`.
3. Check useful-work distribution. Idle consumers beyond the partition count cannot add throughput.
4. Compare PostgreSQL connections and blocked locks with outbox age and Kafka result lag.
5. If intake is above the verified envelope, reduce it and preserve idempotency keys for uncertain HTTP outcomes.
6. Prefer explicit HTTP 429 admission shedding over allowing pool exhaustion to produce ambiguous `5xx` outcomes.
7. Change one capacity variable, rerun the identical profile, and keep both reports.

The measured local envelope, bottlenecks, and tuning sequence are in the [capacity model](../performance/CAPACITY_MODEL.md). Reproduce it with:

```powershell
.\scripts\run-capacity-matrix.ps1
```

## Dependency recovery

| Dependency | Expected behavior | First response | Closure evidence |
|---|---|---|---|
| PostgreSQL | No durable transition can commit; calls may fail or wait | Restore database connectivity and inspect locks/pool exhaustion | Readiness returns, queues drain, durable states converge |
| Kafka | Transactional outboxes retain stable command/result IDs | Restore broker/topic health; do not skip offsets | Lag drains; duplicate deliveries are suppressed; no extra terminal transition |
| Redis | Durable admission remains fenced in PostgreSQL | Restore Redis; do not hand-create keys | Redis failures stop rising; reconciliation rebuilds mirrors; zero permit violations |
| Prometheus | Workflow delivery continues without centralized metrics | Use direct health/logs; restore scrape service | Both application targets are healthy and rules evaluate |
| OpenTelemetry Collector | Workflow delivery continues with incomplete traces | Use durable IDs/logs; restore collector | New post-restoration trace batches export |
| Control plane or worker | Stateless process work is reclaimed after leases/redelivery | Preserve logs and restart only failed replicas | Partition ownership stabilizes and durable work completes |

Task retry, timeout, fencing, DLQ replay, and outbox procedures remain in the [Phase 4 reliability runbook](phase-4-reliability-runbook.md). Schedule, permit, Redis, admission, and rebalance procedures remain in the [Phase 5 coordination runbook](phase-5-scheduling-coordination-runbook.md).

## Resilience verification

Run the complete required matrix with isolated dependency state:

```powershell
.\scripts\run-resilience-matrix.ps1
```

Run one scenario while iterating:

```powershell
.\scripts\run-resilience-matrix.ps1 -Scenario redis-loss -SkipBuild -NoCuration
```

The matrix runs PostgreSQL pause, Kafka pause, Redis pause plus state flush, and simultaneous Prometheus/collector pause. Every scenario is required to pass. Raw outputs remain ignored under `load-testing/results`; curated reports redact the host name and are written to `docs/resilience/reports` with an aggregate `docs/resilience/resilience-summary.json`.

Do not publish a report if its workload did not overlap the fault, an injector failed to restore its service, durable execution counts disagree with accepted work, a required threshold failed, or local identifiers were not redacted.

## Incident closure

Close an incident only when:

- the triggering expression and dependent scrape/rule health are normal;
- accepted workflows reconcile with durable terminal state;
- ready tasks and both outboxes are draining or empty;
- Kafka lag and partition ownership stabilize;
- Redis mirrors reconcile without a PostgreSQL permit-limit violation;
- new telemetry is visible after backend restoration; and
- the incident record identifies whether the cause was code, capacity, dependency, configuration, or SLO calibration.
