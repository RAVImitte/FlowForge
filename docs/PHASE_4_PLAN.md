# Phase 4 plan: reliability, retries, and failure recovery

## Objective

Add durable task retries, deadlines, worker leases, fencing, orphan recovery, and dead-letter handling without weakening Phase 3's PostgreSQL correctness boundary or its honest at-least-once delivery model.

Phase 4 distinguishes three different failure classes:

- A task-attempt failure is a workflow decision and may schedule a new attempt.
- A transport failure is an outbox or Kafka delivery concern and must not consume a task attempt.
- An invalid message is a poison-record concern and is routed to a transport DLQ after bounded consumer retries.

## Correctness invariants

- PostgreSQL remains authoritative for workflow, task, attempt, retry, deadline, and lease state.
- Retry eligibility and the next-attempt timestamp are decided and persisted in the same transaction as attempt completion.
- A retry creates a new attempt and command event; replaying one command keeps its stable event ID and never creates another attempt.
- Backoff timestamps are persisted rather than recomputed after restart.
- Jitter is bounded and deterministic for a task/attempt so failover cannot change the schedule.
- Results and heartbeats must match the active attempt number and fencing token.
- Expired leases are recovered with PostgreSQL claims and common workflow-first lock ordering.
- No worker may revive a terminal task or workflow with a late result.
- DLQ publication is durable and observable; poison records are never acknowledged until their DLQ record is broker-acknowledged.
- Default retry policy is one attempt, preserving Phase 3 behavior for existing workflows.

## Retry and timeout model

Each task definition receives an explicit reliability policy:

- `maxAttempts`: total attempts including the first attempt; default `1`.
- `initialBackoff`: delay before attempt two.
- `backoffMultiplier`: exponential growth factor.
- `maxBackoff`: upper bound before jitter.
- `jitterFactor`: bounded deterministic variation from `0.0` to `1.0`.
- `attemptTimeout`: maximum runtime of one attempt.
- `retryableErrorCodes`: optional allow-list; an empty list uses the worker's retryable classification.

Runtime task state adds `RETRY_SCHEDULED`. A failed retryable attempt transitions `RUNNING -> RETRY_SCHEDULED -> READY`; only exhaustion or a non-retryable failure transitions the task and workflow to `FAILED`. `next_attempt_at`, attempt deadline, lease deadline, and fencing token are durable columns with database constraints and due-work indexes.

## Incremental implementation

### Slice 4.1: Reliability policy and durable state foundation

Status: **COMPLETED**

- Add validated retry/backoff/timeout value objects to the domain.
- Extend task-definition API and persistence with an optional reliability policy.
- Add `RETRY_SCHEDULED` and guarded state transitions without changing existing default behavior.
- Add Flyway runtime columns for next-attempt time, attempt deadline, lease deadline, and fencing token.
- Version Kafka contracts compatibly with optional reliability/fencing fields.
- Add domain, API, migration, persistence, and contract-compatibility tests.

Exit: policies round-trip through workflow definitions and execution materialization, invalid policies are rejected, old payload fixtures remain readable, and workflows without a policy still allow exactly one attempt.

### Slice 4.2: Durable retry scheduling and backoff

Status: **COMPLETED**

- Classify task failures as retryable or non-retryable.
- Atomically persist attempt failure and `RETRY_SCHEDULED` with a deterministic backoff timestamp.
- Claim due retries with bounded `FOR UPDATE SKIP LOCKED` batches.
- Create the next attempt and Kafka command only when the persisted timestamp is due.
- Add retry scheduled/started/exhausted lifecycle events and Micrometer metrics.

Exit: retries survive control-plane restarts, concurrent schedulers cannot create duplicate attempts, and exponential backoff/jitter bounds are deterministic.

### Slice 4.3: Attempt deadlines and timeout recovery

Status: **COMPLETED**

- Persist an attempt deadline when work is claimed.
- Add a horizontally safe timeout-reaper loop.
- Route timed-out attempts through the same retry decision as explicit failures.
- Prevent late success from overwriting a timed-out or superseded attempt.
- Add task and workflow timeout metrics and failure-injection tests.

Exit: attempt timeouts are durable, restart-safe, concurrency-safe, and retry or fail the workflow according to policy.

### Slice 4.4: Worker leases, heartbeats, and fencing

Status: **COMPLETED**

- Add a versioned task-heartbeat contract and topic.
- Issue a unique fencing token per attempt command.
- Renew attempt leases through idempotent heartbeat ingestion.
- Recover expired worker leases as orphaned attempts through the retry policy.
- Reject stale heartbeats and results from replaced workers.

Exit: killing a worker cannot leave work permanently running, and a delayed old worker cannot mutate the recovered attempt.

Implementation checkpoint:

- Task commands now carry a per-attempt fencing token and distributed claims persist an initial worker lease.
- Workers publish immediate and periodic versioned heartbeats for the lifetime of handler execution.
- Heartbeat ingestion is transactionally deduplicated and renews only the matching active attempt.
- Expired leases are claimed with workflow-first `FOR UPDATE SKIP LOCKED` ordering and use the common retry path.
- Result ingestion and completion reject missing or stale fencing tokens from distributed workers.
- The complete 117-test Maven reactor passes with no failures, errors, or skipped tests against Rancher Desktop-backed PostgreSQL and Kafka Testcontainers.

### Slice 4.5: Poison messages and dead-letter queues

Status: **COMPLETED**

- Configure bounded consumer retries for deserialization, validation, and persistent-processing failures.
- Add separate versioned command, result, and heartbeat transport DLQ topics.
- Publish raw record metadata, payload, failure class, and correlation identifiers to DLQ records.
- Add a durable exhausted-task dead-letter event through the control-plane outbox.
- Expose DLQ counts, oldest age, exhaustion, and consumer-recovery metrics.

Exit: poison records no longer block partitions indefinitely, DLQ writes are broker-acknowledged before source offsets, and exhausted tasks are durably inspectable.

Implementation checkpoint:

- Command, result, and heartbeat consumers use bounded, configurable exponential redelivery.
- Exhausted poison records retain their raw key, payload, original metadata, correlation headers, failure class, and deterministic source-record ID on separate versioned DLQ topics.
- DLQ publication requires a broker acknowledgement; failed or timed-out publication leaves the source record unrecovered for redelivery.
- Retry exhaustion emits a durable `TASK_DEAD_LETTERED` execution event through the transactional control-plane outbox.
- Consumer delivery, recovery failure, recovered-record, DLQ publication, record-age, retry-exhaustion, and dead-lettered-task metrics are exposed through Micrometer.
- The complete 126-test Maven reactor passes with no failures, errors, or skipped tests against Rancher Desktop-backed PostgreSQL and Kafka Testcontainers.

### Slice 4.6: Resilience verification and operations

Status: **COMPLETED**

- Add restart, broker-outage, database-outage, worker-kill, stale-result, and scheduler-race tests.
- Verify multiple retry schedulers and timeout reapers claim disjoint work.
- Document retry math, failure classification, DLQ replay safety, and recovery runbooks.
- Record the final reliability architecture in ADR-004.

Exit: the complete Maven suite passes with PostgreSQL and Kafka Testcontainers, no infrastructure test is skipped in the verified environment, and Phase 4 roadmap criteria are satisfied.

Implementation checkpoint:

- Replacement-process tests recover pending control-plane commands, durable worker results, and an offline result consumer without losing or reapplying workflow state.
- Broker and database publication failures now verify the complete recovery sequence: known broker failures release claims, database acknowledgement failures retain leases, and the stable event is published after recovery.
- Worker termination and heartbeat loss are covered by durable lease expiry, orphan recovery, a new fencing token, and rejection of stale old-worker results.
- Duplicate and stale result tests prove inbox idempotency and active-attempt fencing under concurrent delivery.
- Two concurrent retry schedulers and two timeout reapers each claim disjoint six-item batches from a twelve-item workload.
- Retry math, failure ownership, incident recovery, and safe DLQ replay are documented in the [reliability runbook](operations/phase-4-reliability-runbook.md); final decisions are recorded in [ADR-004](adr/004-phase-4-reliability-and-recovery.md).
- The complete 128-test Maven reactor passes across 44 suites with no failures, errors, or skipped tests against Rancher Desktop-backed PostgreSQL and Kafka Testcontainers.

## Deliberately deferred

- One-time and recurring workflow schedules, Redis coordination, rate limiting, and backpressure remain Phase 5.
- OpenTelemetry traces, dashboards, load tests, and soak tests remain Phase 6, although Phase 4 adds reliability metrics and preserves correlation identifiers.
- Authentication, tenant isolation, and deployment hardening remain Phase 7.

## Test strategy

- Property-style domain tests for retry bounds, exponential caps, deterministic jitter, and transition matrices.
- PostgreSQL integration tests for atomic failure/retry decisions, due-time claims, lease fencing, and concurrent recovery.
- Kafka integration tests for compatible contracts, heartbeat ordering, redelivery, and DLQ routing.
- Multi-process tests that stop and replace control planes and workers at every transaction boundary.
- A default-policy regression suite proving existing single-attempt workflows retain Phase 3 semantics.
