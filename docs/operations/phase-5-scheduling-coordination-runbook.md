# Phase 5 scheduling and coordination runbook

This runbook covers durable schedules, misfires, Redis recovery, concurrency permits, admission backpressure, and Kafka consumer rebalancing. PostgreSQL is the source of truth. Redis is a reconstructable acceleration layer; never repair workflow correctness by editing Redis keys.

## First response

1. Check control-plane and worker readiness and confirm PostgreSQL, Kafka, and Redis reachability.
2. Determine whether the incident affects correctness-path progress (PostgreSQL or Kafka) or only coordination acceleration (Redis).
3. Correlate by workflow ID, workflow execution ID, schedule ID, trigger ID, task execution ID, event ID, and fencing token.
4. Inspect queue depth and age before raising batch sizes or rate limits.
5. Preserve trigger, inbox, outbox, permit, and rate-bucket rows as incident evidence.

Primary metrics:

- Scheduling: `flowforge.schedules.materialized`, `flowforge.schedules.misfires.skipped`, `flowforge.schedules.triggers.*`, `flowforge.schedules.pending.*`, `flowforge.schedules.loop.failures`
- Redis coordination: `flowforge.coordination.redis.failures`, `flowforge.coordination.reconciliations`, `flowforge.coordination.reconciled.permits`
- Concurrency: `flowforge.concurrency.permits.*`, `flowforge.concurrency.workflow.rejected`, `flowforge.concurrency.task.deferred`, `flowforge.concurrency.reconciliation.failures`
- Admission: `flowforge.admission.saturated`, `flowforge.admission.throttled`, `flowforge.admission.rejected`, `flowforge.admission.queue.*`, `flowforge.admission.retry.after`
- Transport and recovery: the outbox, worker-result, lease, retry, timeout, consumer-recovery, and DLQ metrics documented in the [Phase 4 runbook](phase-4-reliability-runbook.md)

Read-only PostgreSQL checks:

```sql
SELECT status, count(*)
FROM workflow_schedule
GROUP BY status;

SELECT status, count(*), min(created_at) AS oldest
FROM workflow_schedule_trigger
GROUP BY status;

SELECT id, schedule_id, workflow_id, scheduled_fire_at, status,
       attempt_count, claimed_by, claimed_until, error_message
FROM workflow_schedule_trigger
WHERE status IN ('PENDING', 'PROCESSING', 'FAILED')
ORDER BY scheduled_fire_at, id
LIMIT 200;

SELECT resource_key, status, count(*), min(expires_at) AS earliest_expiry
FROM coordination_permit
GROUP BY resource_key, status
ORDER BY resource_key, status;

SELECT bucket_key, available_tokens, capacity, refill_tokens,
       refill_period_ms, state_version, updated_at
FROM admission_rate_bucket
ORDER BY bucket_key;

SELECT workflow_execution_id, status, count(*), min(created_at) AS oldest
FROM task_execution
WHERE status = 'READY'
GROUP BY workflow_execution_id, status
ORDER BY oldest;
```

## Schedule trigger backlog

Expected behavior: every logical fire has a unique `(schedule_id, scheduled_fire_at)` record and deterministic workflow-start idempotency key. Scheduler replicas claim disjoint bounded batches; expired processing leases are reclaimable.

1. Verify `FLOWFORGE_SCHEDULING_ENABLED=true` on at least one healthy control plane.
2. Compare pending depth and oldest age with the materialization and processing batch sizes.
3. Check `claimed_until` for `PROCESSING` triggers. A replacement scheduler must wait for the lease; it must not clear another replica's claim.
4. If claims repeatedly return to `PENDING`, inspect `error_message`, PostgreSQL availability, workflow state, and admission metrics.
5. Scale scheduler replicas only after checking database lock latency and connection-pool saturation. `FOR UPDATE SKIP LOCKED` makes replicas active-active.
6. Never insert a replacement trigger or workflow execution manually. Restore the failed dependency and let the durable lease/idempotency path recover it.

Relevant configuration:

- `FLOWFORGE_SCHEDULING_MATERIALIZATION_BATCH_SIZE`
- `FLOWFORGE_SCHEDULING_PROCESSING_BATCH_SIZE`
- `FLOWFORGE_SCHEDULING_POLL_INTERVAL_MS`
- `FLOWFORGE_SCHEDULING_LEASE_DURATION`
- `FLOWFORGE_SCHEDULING_RETRY_DELAY`
- `FLOWFORGE_MAX_PENDING_SCHEDULE_FIRES`

## Misfires and clock anomalies

`FIRE_ONCE` creates one catch-up trigger for a stale occurrence and advances recurring schedules to their first future occurrence. `SKIP` records the stale trigger as skipped and creates no execution. Neither policy creates an unbounded replay storm.

1. Confirm the stored IANA time zone, cron expression, `next_fire_at`, and misfire policy.
2. Verify host and PostgreSQL clocks before changing `FLOWFORGE_SCHEDULING_MISFIRE_THRESHOLD`.
3. Use trigger history to distinguish an intentional `SKIPPED` decision from a lost fire.
4. For daylight-saving transitions, use the persisted instant as evidence; do not reinterpret old fires using the current offset.
5. Change a policy through the versioned schedule API with `If-Match`. Do not update `next_fire_at` directly.

## Redis outage, eviction, or restart

Expected behavior: PostgreSQL continues to serialize permits and token-bucket consumption. Redis failures reduce visibility or throughput but cannot create capacity beyond the durable limit.

1. Restore Redis connectivity and confirm the environment namespace is correct.
2. Watch `flowforge.coordination.redis.failures` stop increasing.
3. Keep `FLOWFORGE_CONCURRENCY_RECONCILIATION_ENABLED=true`; active PostgreSQL execution and attempt ownership rebuilds missing permit keys.
4. The next PostgreSQL token-bucket decision rewrites a missing rate-limit mirror with a monotonic state version.
5. Verify rebuilt keys have TTLs. Treat keys without TTLs as invalid operational state and investigate the writer; do not make them persistent.
6. Do not promote Redis from a replica snapshot as a correctness recovery step. It is safe to start empty.

Redis key values are implementation details. Never release a permit, raise capacity, or refill a bucket by editing a key.

## Concurrency saturation or leaked permits

Expected behavior: workflow admission returns HTTP 429 when its immutable version limit is full; task work remains durably `READY` when task capacity is full. Terminal transitions release both task and workflow permits.

1. Compare active durable permits with running workflow executions and task attempts.
2. Check lease, timeout, retry, cancellation, and result-consumer health before diagnosing a leak.
3. Verify the concurrency reconciliation loop is enabled and its failure counter is stable.
4. Allow reconciliation to renew live ownership, replace expired tokens, and release orphaned ownership.
5. A stale worker result or heartbeat must remain fenced. Never reattach its old token.
6. Change concurrency limits by publishing a new workflow version; running executions retain their versioned policy.

## Admission overload and rate limiting

Expected behavior: work is delayed before queues become unbounded. API callers receive HTTP 429 and `Retry-After`; durable schedule fires and ready tasks remain recoverable.

1. Honor `Retry-After` with jitter and cap client retries.
2. Identify the saturated scope from metrics: `schedule-pending`, `schedule-fire`, `task-ready`, or `task-dispatch`.
3. Check oldest queue age as well as depth. A flat depth with rising age indicates stalled consumers; growing depth with stable age usually indicates sustained excess load.
4. Restore downstream capacity before increasing token-bucket refill rates or queue limits.
5. Change capacity gradually and watch PostgreSQL transaction latency, Kafka lag, worker utilization, and DLQ rates.
6. Do not delete pending triggers or ready tasks to reduce a dashboard value.

Key controls:

- `FLOWFORGE_SCHEDULE_RATE_CAPACITY`, `FLOWFORGE_SCHEDULE_RATE_REFILL_TOKENS`, `FLOWFORGE_SCHEDULE_RATE_REFILL_PERIOD`
- `FLOWFORGE_DISPATCH_RATE_CAPACITY`, `FLOWFORGE_DISPATCH_RATE_REFILL_TOKENS`, `FLOWFORGE_DISPATCH_RATE_REFILL_PERIOD`
- `FLOWFORGE_MAX_READY_TASKS_PER_WORKFLOW`, `FLOWFORGE_ADMISSION_RETRY_AFTER`

## Worker scaling and Kafka rebalancing

Workers and control-plane consumers use cooperative sticky partition assignment. Kafka offsets are acknowledged only after the local database transition commits.

1. Add or remove one replica at a time and watch consumer-group lag and partition ownership converge.
2. Keep unique worker and control-plane instance IDs while preserving their consumer-group IDs.
3. Ensure Kafka `max.poll.interval.ms` exceeds the longest supported handler execution. FlowForge's built-in delay handler is bounded at 60 seconds.
4. Allow at least `FLOWFORGE_SHUTDOWN_TIMEOUT` for graceful shutdown. The worker default is 70 seconds so a bounded built-in handler can commit and acknowledge before its partition is revoked.
5. If a process dies instead, expect offset redelivery. Worker inbox identity and stable result IDs suppress duplicate durable completion; attempt fencing rejects obsolete results.
6. Never reset offsets to resolve a rebalance. Investigate poll stalls, database latency, and handler bounds first.

## Restart matrix

| Restarted component | Durable recovery behavior |
|---|---|
| Scheduler/control plane | Expired trigger and outbox leases are reclaimed; deterministic keys suppress duplicate starts |
| Worker | Kafka reassigns partitions; command inbox and stable result IDs suppress duplicate completion |
| PostgreSQL | No state transition is acknowledged without commit; consumers redeliver and publishers replay after recovery |
| Kafka | Outbox records remain pending or leased and replay with stable event IDs after broker recovery |
| Redis | PostgreSQL remains authoritative; permit reconciliation and subsequent bucket decisions rebuild TTL-bounded keys |
| Multiple components | Restore PostgreSQL, Kafka, then application progress; Redis may return empty and rebuild |

After any restart, verify one durable execution per logical schedule fire, no overlapping active permit beyond policy, declining queue age, and no sustained stale-acknowledgement or reconciliation failures.

## Verification

From the repository root with Rancher Desktop or another Docker-compatible runtime available:

```powershell
.\mvnw.cmd verify
```

The suite covers abandoned schedule-claim takeover, control-plane and worker replacement, offline result consumers, Redis permit reconstruction, cooperative worker rebalance during an in-flight handler, exactly-once durable completion, fencing, and multi-replica capacity enforcement using PostgreSQL, Kafka, and Redis Testcontainers.
