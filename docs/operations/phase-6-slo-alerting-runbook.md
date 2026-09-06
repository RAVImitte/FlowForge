# Phase 6 SLO and Alerting Runbook

## Scope and policy

These are initial production-readiness objectives for the FlowForge portfolio deployment. They use a rolling 30-day objective window and must be recalibrated with the reproducible capacity evidence from Phase 6 Slice 6.5. SLOs describe user-visible health; alerts are deliberately split between error-budget burn and direct system symptoms.

| SLI | Good event or sample | Initial objective | Paging signal |
|---|---|---:|---|
| API availability | A non-actuator control-plane HTTP request does not return `5xx` | 99.9% | Multi-window error-budget fast burn |
| Workflow start latency | A successful `POST /api/v1/workflows/{workflowId}/executions` (`202`) finishes within 1 second | 99% | Multi-window latency-budget fast burn |
| Task queue latency | The oldest durable `READY` task is no older than 30 seconds | 99% of 15-second samples | Threshold persists for 5 minutes |
| Terminal completion | The oldest active execution is no older than 15 minutes | 99% of 15-second samples | Threshold persists for 5 minutes |

Availability treats `4xx`, including admission rejection, as available responses because the service completed the request contract; admission saturation has its own alert. Workflow-start latency considers successful starts only, keeping correctness failures in the availability SLI. Actuator traffic is excluded. The queue and terminal SLIs use durable PostgreSQL state, so they remain meaningful across process restarts. They are health SLIs rather than exact per-item latency distributions; exact event-based distributions can be added when Phase 6.5 supplies representative workload classes.

Prometheus retains only bounded labels such as `application`, `environment`, HTTP route, outcome, and scope. Workflow, execution, task, attempt, event, and fencing identifiers must never be metric labels; use trace IDs and structured logs for entity-level diagnosis.

## Burn-rate policy

Request-based SLOs use paired long and short windows. Page when either the 1-hour and 5-minute windows both exceed `14.4x` budget burn, or the 6-hour and 30-minute windows both exceed `6x`. Create a ticket when either the 24-hour and 2-hour windows exceed `3x`, or the 3-day and 6-hour windows exceed `1x`. A short `for` duration filters evaluation jitter without materially delaying a sustained incident.

Gauge-based queue and completion objectives use symptom alerts because a ratio derived from sparse samples can hide a single stuck execution. Outbox, scheduler, retry, timeout, permit, admission, DLQ, dependency, and rule-health alerts also use direct symptoms.

## API availability

1. Confirm the alert environment and whether one or all control-plane instances are affected.
2. Compare `5xx` rate with `up`, PostgreSQL connectivity, Kafka publisher failures, and Redis coordination failures.
3. Use trace IDs from failing requests to locate the first failing dependency and durable state transition.
4. If failures are caused by overload, reduce intake; do not bypass admission or idempotency controls.
5. Resolve the dependency or application fault, then confirm both burn windows fall below threshold.

## Workflow start latency

1. Compare the start latency histogram with PostgreSQL connection-pool pressure, lock waits, admission metrics, and control-plane outbox age.
2. Inspect slow-request traces for time spent in workflow lookup, idempotency acquisition, execution persistence, and outbox persistence.
3. Verify repeated client retries use the same idempotency key.
4. Reduce intake if the durable database path is saturated. Increase replicas only after confirming the bottleneck is not PostgreSQL or its pool.
5. Confirm successful starts return to the one-second objective and the long-window burn is decreasing.

## Task queue latency

1. Check whether workers are up and consuming the expected Kafka partitions.
2. Compare ready depth and age with control-plane outbox age, worker processing count, result-outbox age, and admission metrics.
3. Check Kafka consumer lag and partition ownership. A new worker cannot help when partition count is the limiting factor.
4. Check execution and task coordination permits for saturation or stale leases.
5. Preserve durable rows. Recovery loops and idempotent consumers should drain work after the dependency is restored.

## Terminal completion

1. Query the oldest active execution and inspect its durable task states and latest execution events.
2. Follow the execution trace through command publication, worker processing, result publication, retry, and timeout recovery.
3. Check for an aging ready task, a running attempt beyond its timeout, exhausted retry, stale outbox row, or unavailable worker.
4. Use the supported cancellation or replay paths. Do not edit workflow/task status or fencing tokens manually.
5. If intentionally long workflows are introduced, define a separate workload-class SLO before raising this global threshold.

## Dependency or service loss

1. Confirm the process and scrape port (`8080` control plane, `8081` worker) and inspect `/actuator/health` locally.
2. On Rancher Desktop, verify the Prometheus container resolves the native `host.docker.internal` address. Do not force a Docker `host-gateway` mapping because that can select the wrong Rancher Desktop gateway.
3. Check PostgreSQL, Kafka, and Redis health in `docker compose ps` and review application startup errors.
4. Restart only the failed stateless process after preserving logs; durable PostgreSQL/outbox state enables recovery.
5. Confirm `up` returns to `1`, pending queues drain, and no duplicate terminal transitions occurred.

## Prometheus rule health

Validate configuration and rules with the pinned image from the repository root:

```powershell
docker run --rm --entrypoint /bin/promtool `
  --mount type=bind,source="$((Resolve-Path observability/prometheus/rules).Path)",target=/rules,readonly `
  prom/prometheus:v3.13.0 check rules `
  /rules/flowforge-slo-recording.rules.yml /rules/flowforge-alerts.rules.yml

docker compose --profile observability config --quiet
docker compose --profile observability up -d prometheus
Invoke-RestMethod http://localhost:9090/api/v1/rules
```

For a rule-evaluation failure, inspect the Prometheus log, run `promtool`, and verify the referenced metric name at the source application's `/actuator/prometheus` endpoint. Fix the checked-in rule and reload or restart Prometheus; do not silence evaluation failures.

## Alert ownership and closure

- `page`: acknowledge immediately and mitigate ongoing user-visible risk.
- `warning`: investigate during the active support window before backlog or data-path risk escalates.
- `ticket`: plan remediation while budget remains; close only with evidence that both paired windows recovered.

An incident is complete when the triggering expression is healthy, durable queues are draining or empty, affected workflows reached the correct terminal state, and follow-up evidence identifies whether code, capacity, dependency, or SLO calibration must change.
