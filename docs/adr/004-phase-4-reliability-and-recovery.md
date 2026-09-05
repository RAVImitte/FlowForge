# ADR-004: Durable retries, leases, fencing, and dead-letter recovery

## Status

Accepted

## Context

At-least-once delivery creates three independent failure domains: a task can fail, infrastructure can interrupt message delivery, and a poison record can repeatedly fail consumption. Treating all three as task retries would consume attempts for platform failures and could either lose work or run it without a bound.

Phase 4 must recover after control-plane, worker, Kafka, and PostgreSQL interruptions while allowing every scheduler and recovery loop to run on multiple replicas.

## Decision

PostgreSQL remains the correctness boundary. Kafka transports immutable, versioned messages; it does not own workflow state.

### Task attempts

- A published workflow version captures each task's retry policy.
- Attempt failure, retry classification, next-attempt time, task state, lifecycle events, and outbox records commit in one transaction.
- Backoff is exponential, capped, and deterministically jittered from task-run ID and completed-attempt number.
- `next_attempt_at` and `attempt_deadline` are persisted. Replacement processes never recalculate past decisions.
- A due retry changes `RETRY_SCHEDULED` to `READY`; claiming `READY` creates the next attempt and its command.
- Timeout, explicit retryable failure, and expired worker lease share the same retry/exhaustion path.

### Worker ownership

- Every attempt receives a random fencing token and a renewable lease deadline.
- Commands, heartbeats, and results carry the attempt number and token.
- Only the active running attempt with the matching token can renew its lease or complete its task.
- An expired lease is treated as an orphaned attempt. A delayed old worker is fenced even if it later reconnects.

### Horizontal coordination

- Retry schedulers, timeout reapers, lease reapers, and outbox publishers use bounded `FOR UPDATE SKIP LOCKED` batches.
- Workflow rows are locked before task/attempt mutation to preserve common lock ordering.
- Replicas need no leader election: PostgreSQL assigns disjoint work and state predicates make repeated scans harmless.

### Transport failures and poison records

- A known publish failure releases its outbox claim for retry. An unknown broker outcome or a Kafka acknowledgement that cannot be persisted retains the claim until lease expiry.
- Every replay keeps the stable event ID. Downstream inbox uniqueness makes duplicate state application idempotent.
- Consumer failures use bounded exponential redelivery. Exhausted command, result, and heartbeat records go to separate versioned DLQ topics.
- The DLQ write must be acknowledged by Kafka before the source offset is recovered. A crash between those acknowledgements can duplicate a deterministic DLQ record ID, never silently lose the source record.
- Retry exhaustion is a workflow event (`TASK_DEAD_LETTERED`); transport DLQs are Kafka records. They are deliberately separate concepts.

Task handlers remain responsible for external-side-effect idempotency using the command event ID. A database transaction cannot atomically commit a charge, email, or third-party API call.

## Failure classification

| Failure | Owner | Consumes an attempt | Recovery |
|---|---|---:|---|
| Handler reports retryable failure | Workflow policy | Yes | Persist delayed retry or exhaust |
| Handler reports non-retryable failure | Workflow policy | Yes | Fail task and workflow |
| Attempt deadline expires | Control plane | Yes | Common retry/exhaustion path |
| Worker lease expires | Control plane | Yes | Fence orphan and use common retry path |
| Kafka/PostgreSQL publish interruption | Transport outbox | No | Release or lease-expiry replay |
| Duplicate command/result/heartbeat | Inbox/state predicate | No | Return stored outcome or no-op |
| Invalid or persistently failing consumed record | Consumer recovery | No | Bounded retry, then transport DLQ |

## Consequences

FlowForge provides durable retry timing, bounded execution, orphan recovery, poison-record isolation, and active-active recovery loops without a separate coordinator. Delivery remains honestly at least once, so duplicate records and duplicate external calls are possible unless handlers use the supplied idempotency key.

The design adds durable rows, periodic scans, more lifecycle states, and operational DLQ handling. PostgreSQL availability is required for progress; Redis is not part of the correctness path. Operators must monitor retry, lease, timeout, outbox, and DLQ signals and follow the [Phase 4 reliability runbook](../operations/phase-4-reliability-runbook.md).
