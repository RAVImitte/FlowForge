# Phase 3 plan: Kafka and distributed workers

## Objective

Replace Phase 2's in-process delivery path with durable, at-least-once Kafka delivery and an independently deployable worker service without weakening PostgreSQL workflow-state guarantees.

Phase 3 demonstrates event-driven execution, transactional outbox delivery, consumer inbox deduplication, Kafka consumer-group coordination, crash recovery, and horizontal worker scaling.

## Delivery semantics

- PostgreSQL remains the source of truth for workflow and task state.
- A workflow-state transition and its outbound message are committed in one PostgreSQL transaction.
- Outbox publishers send records to Kafka and mark them published only after broker acknowledgement.
- A publisher crash after the Kafka acknowledgement but before the database update may produce a duplicate.
- Every message carries a stable event ID; consumers use a unique inbox key to make duplicate delivery harmless.
- The platform guarantees at-least-once transport with idempotent processing, not misleading end-to-end exactly-once claims.
- Kafka record keys preserve per-task or per-workflow ordering while still allowing unrelated workflows to execute concurrently.

## Target architecture

### Modules

- `flowforge-messaging`: framework-light, versioned command and event contracts plus compatibility tests.
- `flowforge-control-plane`: owns workflow state, the transactional outbox, command publication, and result ingestion.
- `flowforge-worker`: independently deployable Spring Boot process that consumes task commands and publishes results.

The domain and application modules remain independent of Kafka and Spring.

### Topics

| Topic | Key | Producer | Consumer |
|---|---|---|---|
| `flowforge.task.commands.v1` | task execution ID | control plane | worker group |
| `flowforge.task.results.v1` | workflow execution ID | workers | control-plane group |
| `flowforge.execution.events.v1` | workflow execution ID | control plane | observers and future integrations |

Messages use a common envelope containing `eventId`, `eventType`, `schemaVersion`, `occurredAt`, `correlationId`, and payload. Topic names and schemas are versioned explicitly.

### Transactional outbox and inbox

The control-plane outbox stores the topic, record key, event type, schema version, JSON payload, creation time, publication state, and delivery attempts. Publishers claim bounded batches with PostgreSQL locking so multiple control-plane instances can run concurrently.

The result consumer inserts `(consumer_name, event_id)` into an inbox and applies the task result in the same PostgreSQL transaction. A duplicate event finds the existing inbox key and acknowledges without applying a second state transition.

The worker uses a durable command inbox and result outbox. This allows a restarted worker to reuse the result for a duplicate command instead of executing the handler again. Task handlers receive the command ID as their idempotency token for external side effects.

## Incremental implementation

### Slice 3.1: Kafka foundation and contracts

- Add `flowforge-messaging` and `flowforge-worker` modules.
- Add the Spring Boot Kafka starter managed by Spring Boot 4.1.
- Add a single-node KRaft broker to the local Compose environment.
- Define topic properties and versioned message envelopes.
- Add JSON serialization and backward-compatibility contract tests.
- Add Kafka and worker health indicators.

Exit: the control plane and worker start against local Kafka, topics exist, and contract round-trip tests pass.

### Slice 3.2: Control-plane transactional outbox

- Add Flyway tables for the control-plane outbox.
- Write task commands and lifecycle events in the same transactions as state changes.
- Claim unpublished rows in bounded batches using `FOR UPDATE SKIP LOCKED`.
- Publish with stable event IDs, idempotent producer settings, acknowledgements, and metrics.
- Recover unpublished rows after a control-plane restart.

Exit: a committed task transition always has a recoverable outbox record, and crash/replay tests demonstrate possible duplicate delivery without message loss.

### Slice 3.3: Distributed worker

- Consume task commands as the `flowforge-workers-v1` consumer group.
- Persist a durable worker inbox before execution.
- Move deterministic `NOOP`, `DELAY`, and `FAIL` handlers behind worker application ports.
- Persist task results to a worker outbox and publish them to Kafka.
- Reuse stored results for duplicate commands.
- Verify two worker instances share partitions through consumer-group coordination.

Exit: handlers run outside the control plane and duplicate commands do not repeat completed work.

### Slice 3.4: Result ingestion and DAG advancement

- Consume task results as the `flowforge-control-plane-results-v1` group.
- Insert the result inbox record and apply task completion atomically.
- Reject stale or conflicting task state versions.
- Release downstream DAG tasks and enqueue their commands in the same transaction.
- Publish workflow and task lifecycle events through the outbox.

Exit: linear, fan-out, fan-in, failure, and cancellation workflows complete through Kafka with duplicate results applied once.

### Slice 3.5: Recovery, scaling, and cutover

- Feature-flag the in-process Phase 2 dispatcher and default production mode to Kafka.
- Add PostgreSQL and Kafka Testcontainers integration tests.
- Add restart tests for outbox publishers, consumers, and workers.
- Add multi-instance tests for outbox claims and consumer-group work sharing.
- Document topic configuration, partition-key decisions, local operation, and failure semantics.
- Record the final architecture in ADR-003.

Exit: the full Maven suite passes, no Kafka integration test is skipped in the verified environment, and Phase 3 exit criteria in the roadmap are satisfied.

## Deferred deliberately

- Retry policy, exponential backoff, timeouts, poison-message handling, and DLQs remain Phase 4.
- Worker leases, heartbeats, fencing, and orphan recovery remain Phase 4.
- Redis coordination, scheduling, rate limits, and backpressure remain Phase 5.
- OpenTelemetry propagation and production dashboards remain Phase 6, although message correlation fields and baseline metrics are introduced now.

## Test strategy

- Unit tests for envelopes, serializers, outbox claims, and inbox decisions.
- PostgreSQL integration tests for atomic state/outbox and inbox/state transactions.
- Kafka integration tests for keys, headers, serialization, group coordination, and redelivery.
- End-to-end tests with PostgreSQL, Kafka, one control plane, and multiple workers.
- Failure injection around publish acknowledgement, process restart, duplicate commands, and duplicate results.
