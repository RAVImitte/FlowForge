# Phase 5 plan: durable scheduling, coordination, and backpressure

## Objective

Add one-time and recurring workflow schedules, horizontally safe trigger materialization, Redis-assisted coordination, concurrency limits, and admission control without moving correctness out of PostgreSQL.

Redis is an optimization and short-lived coordination layer. A Redis restart may reduce throughput temporarily, but it must never lose a schedule, create an unbounded duplicate execution, or make durable workflow state unrecoverable.

## Correctness invariants

- PostgreSQL is authoritative for schedule definitions, next-fire timestamps, trigger history, execution idempotency, and durable concurrency state.
- Every schedule fire has a deterministic idempotency key derived from schedule ID and scheduled fire time.
- Claiming a due schedule and recording its trigger decision use bounded database transactions and token-fenced leases.
- Recurring schedules persist their next fire time; replacement processes do not infer missed history from wall-clock scans alone.
- Time zones are explicit IANA zone IDs and daylight-saving transitions have deterministic behavior.
- Misfire behavior is explicit: catch up once or skip to the next future occurrence. FlowForge never creates an unbounded catch-up storm.
- Multiple scheduler replicas may run simultaneously and must claim disjoint due work.
- Redis permit keys are leased and token-owned; rate-limit keys are versioned, TTL-bounded, and reconstructable from PostgreSQL state.
- Admission control rejects or delays work before queues and connection pools become unbounded.
- Schedule pause, resume, update, and deletion use optimistic concurrency.

## Incremental implementation

### Slice 5.1: Durable schedule-definition foundation

Status: **COMPLETED**

- Add framework-free one-time and cron schedule specifications with time-zone and misfire-policy semantics.
- Add Flyway-managed schedule-definition and trigger-history tables.
- Add create, inspect, list, pause, resume, update, and delete application ports and HTTP APIs.
- Validate that schedules target active workflows with a published version.
- Use strong ETags for schedule mutations and add domain, API, migration, and PostgreSQL persistence tests.

Exit: valid schedule definitions round-trip durably, invalid expressions/timestamps/zones are rejected, mutations are concurrency-safe, and creating a schedule does not yet start an execution.

Implemented checkpoint:

- Added framework-free one-time and cron schedule models with explicit IANA time zones and misfire policies.
- Added durable PostgreSQL schedule definitions and trigger-history foundations through Flyway migration V7.
- Added create, inspect, list, update, pause, resume, and soft-delete APIs with strong ETag concurrency control.
- Restricted schedules to active workflows that have a published version.
- Verified schedule domain rules, cron calculation, HTTP behavior, PostgreSQL round trips, lifecycle changes, and stale-version rejection.
- Verified the complete Maven reactor: 137 tests across 48 suites, with no failures, errors, or skipped tests.

### Slice 5.2: Due-fire materialization and misfire recovery

Status: **COMPLETED**

- Compute and persist next fire times for one-time and recurring schedules.
- Claim due schedules with bounded `FOR UPDATE SKIP LOCKED` batches and fenced leases.
- Persist each fire in trigger history and start its workflow with a deterministic idempotency key.
- Implement catch-up-once and skip misfire policies with bounded recovery.
- Add restart, clock-boundary, daylight-saving, and competing-scheduler tests.

Exit: scheduler replicas materialize each logical fire once at the workflow-state boundary and recover safely after downtime.

Implemented checkpoint:

- Added a production-only scheduler loop with bounded materialization and processing batches.
- Materialized due schedules with PostgreSQL `FOR UPDATE SKIP LOCKED` claims and deterministic schedule/fire idempotency keys.
- Separated durable trigger creation from workflow start so an interrupted processor can safely replay the same execution request.
- Added token-fenced trigger leases, expired-claim recovery, retry delay, and terminal failure recording.
- Completed one-time schedules after their only occurrence and advanced recurring schedules to their first future occurrence.
- Implemented configurable misfire threshold behavior: `FIRE_ONCE` catches up once and `SKIP` records a skipped trigger without creating an execution.
- Added scheduler metrics for materialization, skipped misfires, claims, starts, failures, releases, stale acknowledgements, and loop failures.
- Verified DST-gap calculation, deterministic fire identity, transient/permanent failure handling, competing schedulers, lease recovery, and stale-token rejection.
- Verified the complete Maven reactor: 146 tests across 50 suites, with no failures, errors, or skipped tests.

### Slice 5.3: Redis coordination foundation

Status: **COMPLETED**

- Add Redis to the local environment and health/metrics configuration.
- Implement token-owned leased permits and atomic Lua acquire/renew/release operations.
- Namespace keys by environment and resource; attach bounded TTLs to every coordination key.
- Reconcile Redis state from PostgreSQL after key eviction, restart, or failover.
- Add Redis Testcontainers and failure-injection tests.

Exit: Redis improves coordination latency but its total loss cannot violate durable execution or schedule correctness.

Implemented checkpoint:

- Added ephemeral Redis 7.4 to the local Compose environment and production-only coordination activation.
- Added a PostgreSQL permit ledger that serializes capacity decisions per resource and expires leases durably.
- Implemented atomic Redis Lua acquire, renew, release, replacement, expiry cleanup, and active-count operations.
- Added token ownership, bounded lease durations, bounded key TTLs, environment namespaces, and hashed resource keys.
- Kept PostgreSQL authoritative: Redis failures are metered and do not revoke a successfully persisted permit.
- Added explicit reconstruction from active PostgreSQL permits after Redis key eviction, restart, or failover.
- Added Redis health configuration and metrics for degraded operations, reconciliations, and rebuilt permit counts.
- Verified concurrent PostgreSQL and Redis capacity enforcement, holder idempotency, stale-token rejection, expiry, complete Redis key loss, and outage-safe acquisition.
- Verified the complete Maven reactor: 156 tests across 53 suites, with no failures, errors, or skipped tests.

### Slice 5.4: Workflow and task concurrency limits

Status: **COMPLETED**

- Add versioned per-workflow and per-task concurrency policies.
- Acquire permits before dispatch and release them on every terminal, timeout, and orphan-recovery path.
- Use PostgreSQL state as the reconciliation source for leaked or missing Redis permits.
- Prevent permit oversubscription across control-plane and worker replicas.

Exit: configured limits hold under concurrent dispatch, crashes, retries, and Redis recovery.

Implemented checkpoint:

- Added optional, validated concurrency policies to immutable workflow versions and individual task definitions through Flyway migration V10 and the workflow HTTP contract.
- Acquired token-owned PostgreSQL permits before workflow admission and task dispatch, with active execution state checked under the same resource lock.
- Preserved start idempotency during concurrent admission and returned an explicit HTTP 429 response when a workflow-version limit is saturated.
- Left task work durably `READY` when its task-version capacity is full so another dispatch pass can claim it after release.
- Released task permits on normal completion, retry scheduling, terminal timeout, cancellation, and expired worker-lease recovery; released workflow permits on every terminal transition.
- Added a production reconciliation loop that renews live ownership, replaces expired tokens, releases orphaned permits, and rebuilds the Redis mirror from PostgreSQL execution state.
- Added bounded metrics for permits acquired/released, workflow rejections, task deferrals, reconciliation work, and reconciliation failures.
- Verified concurrent multi-replica admission, task deferral, retry/timeout/orphan release, permit expiry recovery, Redis key loss, and versioned API persistence.
- Verified the complete Maven reactor: 165 tests across 53 suites, with no failures, errors, or skipped tests.

### Slice 5.5: Rate limiting and admission backpressure

Status: **COMPLETED**

- Add atomic token-bucket rate limiting for schedule fires and task dispatch.
- Bound pending scheduled work and per-workflow ready queues.
- Return explicit overload responses with retry guidance at API admission points.
- Expose saturation, throttling, queue-age, and rejected-admission metrics.

Exit: overload is visible and bounded; the platform sheds or delays work according to policy instead of exhausting resources.

Implemented checkpoint:

- Added PostgreSQL-authoritative token buckets with fractional refill, atomic row locking, partial batch grants, and precise retry guidance across replicas.
- Applied the shared dispatch bucket before in-process claims, polling outbox enqueueing, and result-driven downstream command enqueueing; applied a separate bucket before schedule-trigger claims.
- Added versioned, TTL-bounded Redis bucket mirrors whose monotonic state versions prevent stale after-commit callbacks from overwriting newer state; PostgreSQL consumption rebuilds missing keys.
- Bounded pending schedule-trigger materialization with a transaction-scoped capacity lock and bounded per-workflow ready queues with serialized admission and DAG promotion.
- Returned explicit HTTP 429 responses with `Retry-After` headers for both workflow-concurrency and ready-queue saturation.
- Added bounded saturation, throttling, rejected-admission, queue-depth, queue-age, and retry-after metrics.
- Verified fractional refill, concurrent capacity enforcement, Redis key reconstruction, pending-queue bounds, ready-queue rejection, scheduler replica safety, and HTTP retry guidance.
- Verified the complete Maven reactor: 173 tests across 54 suites, with no failures, errors, or skipped tests.

### Slice 5.6: Rebalancing, resilience, and operations

Status: **COMPLETED**

- Verify scheduler, control-plane, worker, PostgreSQL, Kafka, and Redis restart combinations.
- Test graceful consumer rebalancing while permits and scheduled fires are in flight.
- Add scheduling, misfire, Redis-recovery, concurrency, and overload runbooks.
- Record final Phase 5 architecture in ADR-005.

Exit: the complete Maven suite passes with PostgreSQL, Kafka, and Redis Testcontainers and no infrastructure skips in the verified environment.

Implemented checkpoint:

- Configured cooperative sticky Kafka assignment for worker and control-plane consumers and explicit graceful-shutdown budgets.
- Verified a worker-group join while a command handler was in flight: the original owner committed and acknowledged exactly once before balanced partition ownership converged.
- Extended the multi-process restart test from a deliberately abandoned schedule claim through replacement scheduling, transactional outbox delivery, worker replacement, offline result publication, and replacement result consumption.
- Verified workflow and task permits remain PostgreSQL-authoritative and are reconstructed in Redis after complete key loss while the scheduled execution remains active.
- Verified deterministic schedule execution identity, trigger fencing, durable inbox/outbox deduplication, and final permit release across the restart sequence.
- Added the Phase 5 scheduling, misfire, Redis recovery, concurrency, overload, rebalance, and restart runbook.
- Recorded the final durable scheduling and PostgreSQL-authoritative coordination design in ADR-005.
- Verified the complete Maven reactor: 174 tests across 54 suites, with no failures, errors, or skipped tests.

Phase 5 exit criteria are satisfied. Next: **Phase 6 - Observability and horizontal scalability**.

## Deliberately deferred

- OpenTelemetry traces, dashboards, capacity/load models, and soak testing remain Phase 6.
- Authentication, tenant quotas, deployment manifests, and delivery automation remain Phase 7.

## Test strategy

- Domain tests for schedule forms, time zones, misfire policies, and DST boundaries.
- PostgreSQL tests for optimistic mutation, deterministic fire identity, disjoint claims, and restart recovery.
- Redis tests for atomic permits, fencing, expiry, reconstruction, and fail-open/fail-closed decisions.
- End-to-end tests for scheduled Kafka execution, concurrency limits, retry/timeout permit release, and overload behavior.
- Multi-process tests that stop and replace scheduler/control-plane/worker replicas at transaction boundaries.
