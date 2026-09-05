# ADR-005: Durable scheduling and PostgreSQL-authoritative coordination

## Status

Accepted

## Context

FlowForge needs one-time and recurring execution, concurrency limits, and overload protection while every scheduler, control plane, and worker can scale horizontally or fail independently. Wall-clock-only scheduling loses decisions during downtime. A Redis-only lock or rate limiter can lose ownership during restart or eviction. Unbounded catch-up, ready-task, or trigger queues can turn recovery into another outage.

Phase 5 must therefore preserve each scheduling and admission decision across PostgreSQL, Kafka, Redis, and process interruptions without introducing a singleton coordinator.

## Decision

PostgreSQL remains the correctness boundary. Redis mirrors short-lived coordination state and may start empty.

### Durable schedules and logical fires

- Schedule definitions use explicit one-time instants or cron expressions with IANA time zones and an explicit `FIRE_ONCE` or `SKIP` misfire policy.
- Recurring `next_fire_at` values are persisted. Replacement schedulers continue from durable state rather than reconstructing history from wall time.
- Due schedules are materialized in bounded `FOR UPDATE SKIP LOCKED` batches.
- Trigger history is unique by schedule and scheduled fire time. Its deterministic idempotency key is used to start the workflow.
- Trigger processing uses expiring claims and random fencing tokens. Only the current token can record success, failure, or release.
- A stale occurrence creates at most one catch-up decision. FlowForge never emits an unbounded sequence of missed runs.

### Concurrency permits

- Immutable workflow versions may define a workflow-execution limit and task definitions may define task-attempt limits.
- PostgreSQL serializes each resource's admission and stores token-owned, expiring permits.
- Workflow and task state references its permit token. Terminal, retry, cancellation, timeout, and orphan-recovery transitions release ownership.
- A reconciliation loop derives live ownership from workflow and attempt state, renews or replaces valid permits, retires orphans, and rebuilds Redis.
- Redis Lua operations provide atomic low-latency mirrors with environment-namespaced hashed keys and bounded TTLs. A Redis failure cannot revoke a PostgreSQL admission decision or grant durable excess capacity.

### Rate limiting and bounded admission

- PostgreSQL token-bucket rows use atomic locks, fractional refill, partial grants, and monotonic state versions across replicas.
- Redis receives TTL-bounded, version-checked bucket snapshots after the durable transaction. An older callback cannot overwrite newer state.
- Schedule-trigger materialization has a global pending bound. Each workflow execution has a bounded ready-task queue.
- Schedule claims and every task-dispatch path consume the relevant bucket before claiming work. A denied grant leaves durable work pending.
- API saturation returns HTTP 429 with `Retry-After`; queue depth, age, throttling, saturation, and rejected admission are metered.

### Rebalancing and restart recovery

- Kafka consumers use cooperative sticky partition assignment and manual immediate acknowledgement.
- Worker command receipt and completion commit to PostgreSQL before acknowledgement. Duplicate delivery reuses the durable inbox outcome and stable result event ID.
- Graceful shutdown budgets exceed the bounded built-in handler duration. Abrupt termination still recovers through Kafka redelivery, attempt leases, and fencing.
- Scheduler and publisher replicas use durable token-fenced leases. Replacement processes reclaim only expired ownership.
- Recovery tests cross real process contexts and PostgreSQL, Kafka, and Redis containers: an abandoned schedule is started once, permits survive control-plane replacement and Redis loss, an offline result is later consumed, and a worker-group join completes in-flight work exactly once at the durable boundary.

## Failure semantics

| Failure | Authoritative state | Recovery |
|---|---|---|
| Scheduler exits with a trigger in flight | PostgreSQL trigger claim | Replacement reclaims after lease expiry; stale token is fenced |
| Duplicate logical fire | Trigger uniqueness and workflow idempotency key | Existing trigger/execution wins |
| Worker exits or partition moves | Kafka offset, worker inbox, attempt token | Redelivery is deduplicated or stale work is fenced |
| Redis restarts or evicts keys | Permit ledger, execution state, rate bucket | Reconciliation and subsequent decisions rebuild mirrors |
| PostgreSQL is unavailable | No transition can commit | Stop progress and redeliver/replay after recovery |
| Kafka is unavailable | Transactional outboxes | Retain or reclaim messages and publish stable IDs after recovery |
| Workflow or task capacity is full | Durable permit ledger | Reject workflow admission or leave task `READY` |
| Rate or queue capacity is full | Durable bucket and queue state | Return retry guidance or defer claims/materialization |

## Consequences

FlowForge schedules and coordinates work without leader election, tolerates total Redis state loss, and bounds recovery pressure. Its correctness is explainable from PostgreSQL rows and immutable Kafka identities. Active-active replicas can duplicate delivery and scans, but cannot legitimately apply the same fenced transition twice.

The design adds database locks, trigger and permit retention, periodic reconciliation, and operational tuning across leases, batches, buckets, and queue bounds. PostgreSQL is required for progress and can become the coordination throughput ceiling. Redis improves the ephemeral view but is not a substitute for database capacity planning.

Long-running custom handlers must remain within Kafka poll and shutdown budgets or implement a different execution model. Exactly-once external side effects remain the handler's responsibility using the supplied command event ID.

Operational response is documented in the [Phase 5 scheduling and coordination runbook](../operations/phase-5-scheduling-coordination-runbook.md). Task retry, fencing, transport DLQ, and outbox recovery remain governed by [ADR-004](004-phase-4-reliability-and-recovery.md) and the [Phase 4 runbook](../operations/phase-4-reliability-runbook.md).
