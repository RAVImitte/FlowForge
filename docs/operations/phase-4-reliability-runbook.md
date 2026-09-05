# Phase 4 reliability runbook

This runbook covers task retries, timeouts, worker loss, infrastructure outages, and transport DLQ replay. PostgreSQL is authoritative; do not repair workflow state by editing Kafka offsets or runtime tables manually.

## Retry calculation and classification

For completed attempt `n`, before attempt `n + 1`:

```text
baseMs   = min(maxBackoffMs, initialBackoffMs * backoffMultiplier^(n - 1))
jitter   = deterministic(taskRunId, n) in [1 - jitterFactor, 1 + jitterFactor)
delayMs  = max(0, round(baseMs * jitter))
nextAttemptAt = completionTime + delayMs
```

The deterministic jitter seed ensures every replacement control plane reaches the same delay. The resulting `next_attempt_at` is persisted and is the only schedule used after commit.

`retryableErrorCodes` is an allow-list when non-empty: only a normalized matching code retries. When empty, the worker's `retryable` classification controls the decision. A retry occurs only when the failure is retryable and the completed attempt is below `maxAttempts`. Timeouts and expired leases are retryable inputs to the same policy; transport failures never consume attempts.

## First response

1. Check `/actuator/health/readiness` on control-plane and worker instances.
2. Confirm PostgreSQL and Kafka availability before restarting services.
3. Inspect these metrics together rather than relying on one counter:
   - `flowforge.outbox.pending`, `flowforge.outbox.publish.failures`, `flowforge.outbox.acknowledgement.failures`
   - `flowforge.worker.results.pending`, `flowforge.worker.results.publish.failures`, `flowforge.worker.results.unknown.outcomes`
   - `flowforge.retries.scheduled`, `flowforge.retries.released`, `flowforge.retries.scheduler.failures`
   - `flowforge.timeouts.reaped`, `flowforge.timeouts.reaper.failures`
   - `flowforge.leases.reaped`, `flowforge.leases.reaper.failures`, `flowforge.leases.heartbeat.failures`
   - `flowforge.kafka.consumer.delivery.failures`, `flowforge.kafka.consumer.recovery.failures`, `flowforge.kafka.dlq.published`
4. Correlate logs by workflow execution ID, task execution ID, attempt number, event ID, and fencing token.

Read-only PostgreSQL checks:

```sql
SELECT status, count(*)
FROM control_plane_outbox
GROUP BY status;

SELECT id, workflow_execution_id, task_execution_id, status,
       next_attempt_at, now() - next_attempt_at AS overdue
FROM task_execution
WHERE status = 'RETRY_SCHEDULED'
ORDER BY next_attempt_at;

SELECT ta.task_execution_id, ta.attempt_number, ta.attempt_deadline,
       ta.lease_deadline, now() - ta.lease_deadline AS lease_overdue
FROM task_attempt ta
WHERE ta.status = 'RUNNING'
ORDER BY ta.lease_deadline NULLS LAST;
```

## Kafka outage

Expected behavior: known send failures are released; unknown outcomes remain leased. Consumers resume from committed offsets and publishers replay stable event IDs after Kafka returns.

1. Restore broker quorum and verify the configured bootstrap address from both deployables.
2. Leave control planes and workers running or restart them normally. Do not reset consumer offsets.
3. Watch pending outbox gauges fall after the claim lease duration.
4. Expect duplicates when the pre-outage broker outcome was unknown. Verify inbox duplicate/redundant outcomes rather than deleting them.
5. Escalate if pending counts grow while broker health is up and publish-failure counters continue rising.

## PostgreSQL outage

Expected behavior: no workflow transition is acknowledged without its database commit. Kafka may redeliver consumed records. A Kafka-acknowledged outbox record whose database update failed is replayed after its claim lease expires.

1. Restore the primary and verify schema/Flyway history before enabling traffic after a failover.
2. Confirm application readiness returns and pool connection errors stop.
3. Allow outbox, scheduler, timeout, and lease loops to resume; they are state-predicate and lease protected.
4. Expect stable-ID duplicates from acknowledgement ambiguity. Do not mark outbox rows published manually.
5. Compare execution events and inbox rows before considering any targeted intervention.

## Worker termination or lost heartbeat

Expected behavior: heartbeat renewal stops, the attempt lease expires, one reaper fences the old attempt, and policy either schedules a retry or exhausts the task.

1. Replace the worker process with the same consumer group and a unique worker ID.
2. Wait at least `FLOWFORGE_WORKER_LEASE_DURATION` plus the lease-reaper poll interval.
3. Verify `flowforge.leases.reaped` increments and the task becomes `RETRY_SCHEDULED`, `READY`, or terminal according to policy.
4. Treat results or heartbeats from the old fencing token as stale. Never relax token checks to unblock a task.
5. If no recovery occurs, verify the production profile enables the lease reaper and that PostgreSQL time is sane.

## Retry or timeout backlog

All control-plane replicas may run the loops. `SKIP LOCKED` assigns disjoint batches; no singleton scheduler is required.

- For overdue retries, verify `FLOWFORGE_RETRY_SCHEDULER_ENABLED`, batch size, and poll interval.
- For overdue attempt deadlines, verify `FLOWFORGE_TIMEOUT_REAPER_ENABLED`.
- For expired worker leases, verify `FLOWFORGE_LEASE_REAPER_ENABLED` and heartbeat-consumer health.
- Scale replicas or batch sizes only after checking PostgreSQL lock time, connection-pool saturation, and transaction latency.
- Never set `next_attempt_at`, attempt status, or fencing tokens by hand during normal recovery.

## Transport DLQ triage and safe replay

Topics:

| Source | DLQ |
|---|---|
| `flowforge.task.commands.v1` | `flowforge.task.commands.dlq.v1` |
| `flowforge.task.results.v1` | `flowforge.task.results.dlq.v1` |
| `flowforge.task.heartbeats.v1` | `flowforge.task.heartbeats.dlq.v1` |

The DLQ retains the raw key/value, original Kafka metadata and headers, root failure class, failure timestamp, schema version, and deterministic source identity `topic:partition:offset`.

Replay procedure:

1. Quarantine and inspect the DLQ record. Group duplicates by `flowforge-dlq-record-id`.
2. Fix and deploy the consumer/schema/configuration issue first.
3. Confirm whether the original event ID already exists in the destination inbox or whether the task attempt has been superseded. Stale records are evidence, not replay candidates.
4. Replay the original key and value to its original source topic while preserving event and correlation headers. Never generate a new event ID.
5. Replay a small sample, verify inbox outcome and workflow history, then continue in bounded batches.
6. Retain an audit of source DLQ topic/partition/offset, deterministic DLQ record ID, operator, reason, destination, and replay time.
7. Do not delete or advance DLQ retention solely because replay produced a duplicate; idempotent duplicate handling is the expected safe outcome.

`TASK_DEAD_LETTERED` is not replayed from a transport DLQ. It records workflow-level retry exhaustion and requires a product decision such as starting a new workflow execution after the underlying issue is corrected.

## Verification suite

Run from the repository root with a Docker-compatible runtime available:

```powershell
.\mvnw.cmd verify
```

The suite covers replacement-process restart recovery, publish failures across Kafka and database boundaries, duplicate delivery, stale results, worker lease expiry, bounded poison-record recovery, and disjoint scheduler/reaper claims using PostgreSQL and Kafka Testcontainers.
