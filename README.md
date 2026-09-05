# FlowForge

FlowForge is a production-oriented distributed workflow and job orchestration platform. Phase 5 is underway: durable scheduling, Redis-assisted coordination, and workflow/task concurrency enforcement are complete; rate limiting and admission backpressure are next.

## Current capabilities

- Create, retrieve, list, update, publish, and archive workflow definitions
- Model task dependencies as a validated directed acyclic graph
- Preserve immutable published versions while allowing a new draft
- Prevent lost updates with strong ETags and `If-Match`
- Apply schema changes with Flyway
- Expose liveness, readiness, metrics, and application information
- Verify domain, HTTP, architecture, migration, and persistence behavior
- Start executions from immutable published workflow versions
- Materialize root tasks as ready and dependent tasks as blocked
- Resolve linear, fan-out, fan-in, and mixed DAGs as tasks complete
- Claim ready work safely across instances with `FOR UPDATE SKIP LOCKED`
- Make duplicate starts and task-completion reports idempotent
- Persist task attempts and an ordered execution-event journal
- Cancel workflows without dispatching additional dependent work
- Recover durable ready work through the startup and scheduled dispatch loop
- Execute deterministic `NOOP`, `DELAY`, and `FAIL` handlers in independently scalable workers
- Share framework-light, versioned Kafka command and event contracts
- Provision versioned task-command, task-result, task-heartbeat, and execution-event topics
- Run independently deployable workers with Kafka and worker health checks
- Start a single-node Kafka 4.3 KRaft broker through the local Compose environment
- Persist task commands and execution events through a PostgreSQL transactional outbox
- Claim outbox batches safely across control-plane instances with short, recoverable leases
- Publish stable event IDs, record keys, correlation headers, and versioned JSON envelopes to Kafka
- Recover broker failures, acknowledgement uncertainty, and publisher crashes without message loss
- Expose outbox publication, failure, stale-acknowledgement, and pending-backlog metrics
- Consume task commands through the shared `flowforge-workers-v1` Kafka consumer group
- Persist commands before execution and acknowledge Kafka only after durable completion
- Suppress completed-command duplicates through a durable inbox and stable event IDs
- Commit task completion and a durable result-outbox row atomically
- Publish task results through recoverable, token-fenced PostgreSQL claims
- Scale workers horizontally through Kafka partition assignment
- Consume task results through the `flowforge-control-plane-results-v1` Kafka group
- Apply result inbox records, task transitions, DAG advancement, events, and downstream commands atomically
- Reject stale state versions, mismatched task identities, unknown attempts, and reused event IDs
- Suppress duplicate and concurrent equivalent results without advancing the DAG twice
- Fence every distributed task attempt with a unique token and renewable PostgreSQL lease
- Publish worker heartbeats while handlers run and ingest them idempotently
- Recover orphaned attempts through the durable retry policy with multi-replica-safe lease claims
- Reject delayed heartbeats and results from workers replaced after lease expiry
- Retry poison command, result, and heartbeat records with bounded exponential backoff
- Route exhausted poison records to separate versioned DLQ topics without losing raw payloads or correlation metadata
- Require broker acknowledgement of each DLQ publication before recovering the source offset
- Emit durable `TASK_DEAD_LETTERED` events when task retries are exhausted
- Define one-time and recurring cron schedules with explicit IANA time zones and misfire policies
- Persist schedule definitions and trigger-history foundations in PostgreSQL
- Create, inspect, list, update, pause, resume, and soft-delete schedules through versioned HTTP APIs
- Fence concurrent schedule mutations with strong ETags and `If-Match`
- Materialize due fires in bounded, disjoint PostgreSQL batches across scheduler replicas
- Start scheduled workflows with deterministic logical-fire idempotency keys
- Recover abandoned trigger processing through token-fenced leases and delayed retries
- Catch up once or skip stale occurrences according to an explicit misfire policy and threshold
- Complete one-time schedules and durably advance recurring schedules to their next future occurrence
- Serialize coordination capacity through a PostgreSQL-authoritative permit ledger
- Mirror short-lived permits into Redis with atomic Lua acquire, renew, release, and replacement operations
- Namespace and hash Redis keys while bounding every coordination key with a TTL
- Continue safely through Redis outages and reconstruct ephemeral state from PostgreSQL after key loss
- Persist optional per-workflow-version and per-task concurrency limits with workflow definitions
- Serialize workflow admission and task dispatch across replicas through PostgreSQL-authoritative permits
- Leave saturated tasks durably `READY` and return HTTP 429 for saturated workflow admission
- Release task permits on success, failure, retry, cancellation, timeout, and orphan-lease recovery paths
- Renew active permits, retire leaked ownership, and rebuild Redis state from active PostgreSQL executions
- Expose bounded workflow rejection, task deferral, permit lifecycle, and reconciliation metrics

Phase 4 is complete. Phase 5 Slices 5.1-5.4 are verified across PostgreSQL, Kafka, and Redis. Slice 5.5 adds rate limiting and admission backpressure.

## Prerequisites

- Java 21
- Rancher Desktop using the Moby engine, or another Docker-compatible runtime

The Maven wrapper downloads the pinned Maven version automatically.

## Run locally

Start PostgreSQL, Kafka, and Redis:

```powershell
docker compose up -d postgres kafka redis
```

Run the control plane in its production Kafka topology:

```powershell
$env:JAVA_HOME = 'C:\path\to\jdk-21'
$env:SPRING_PROFILES_ACTIVE = 'production'
.\mvnw.cmd -pl flowforge-control-plane -am spring-boot:run
```

The `production` profile enables Kafka, outbox publication, command dispatch, and result consumption and disables the Phase 2 in-process dispatcher. Startup validation rejects incomplete or competing execution topologies.

Run a worker in a second terminal (it uses the same local PostgreSQL service by default, with an independent Flyway history table):

```powershell
.\mvnw.cmd -pl flowforge-worker -am spring-boot:run
```

Run all tests:

```powershell
.\mvnw.cmd verify
```

Infrastructure integration tests use PostgreSQL and Kafka Testcontainers. They are skipped when a Docker-compatible runtime is unavailable; all other tests still run.

## Configuration

| Environment variable | Default |
|---|---|
| `FLOWFORGE_DB_URL` | `jdbc:postgresql://localhost:5432/flowforge` |
| `FLOWFORGE_DB_USERNAME` | `flowforge` |
| `FLOWFORGE_DB_PASSWORD` | `flowforge` |
| `FLOWFORGE_DB_POOL_SIZE` | `10` |
| `FLOWFORGE_REDIS_HOST` | `localhost` |
| `FLOWFORGE_REDIS_PORT` | `6379` |
| `FLOWFORGE_REDIS_PASSWORD` | Empty |
| `FLOWFORGE_REDIS_CONNECT_TIMEOUT` | `2s` |
| `FLOWFORGE_REDIS_COMMAND_TIMEOUT` | `2s` |
| `FLOWFORGE_DISPATCH_ENABLED` | `true`; production profile: `false` |
| `FLOWFORGE_DISPATCH_INTERVAL_MS` | `250` |
| `FLOWFORGE_KAFKA_ENABLED` | Control plane: `false`; production profile/worker: `true` |
| `FLOWFORGE_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `FLOWFORGE_KAFKA_TOPIC_PARTITIONS` | `6` |
| `FLOWFORGE_KAFKA_TOPIC_REPLICATION_FACTOR` | `1` |
| `FLOWFORGE_KAFKA_RECOVERY_MAX_RETRIES` | `2` |
| `FLOWFORGE_KAFKA_RECOVERY_INITIAL_BACKOFF` | `250ms` |
| `FLOWFORGE_KAFKA_RECOVERY_BACKOFF_MULTIPLIER` | `2.0` |
| `FLOWFORGE_KAFKA_RECOVERY_MAX_BACKOFF` | `2s` |
| `FLOWFORGE_KAFKA_DLQ_PUBLISH_TIMEOUT` | `10s` |
| `FLOWFORGE_OUTBOX_PUBLISHER_ENABLED` | `false`; production profile: `true` |
| `FLOWFORGE_OUTBOX_COMMAND_DISPATCH_ENABLED` | `false`; production profile: `true` |
| `FLOWFORGE_OUTBOX_BATCH_SIZE` | `100` |
| `FLOWFORGE_OUTBOX_POLL_INTERVAL_MS` | `250` |
| `FLOWFORGE_OUTBOX_COMMAND_DISPATCH_INTERVAL_MS` | `250` |
| `FLOWFORGE_OUTBOX_LEASE_DURATION` | `30s` |
| `FLOWFORGE_OUTBOX_PUBLISH_TIMEOUT` | `10s` |
| `FLOWFORGE_RESULT_CONSUMER_ENABLED` | `false`; production profile: `true` |
| `FLOWFORGE_RESULT_CONSUMER_GROUP` | `flowforge-control-plane-results-v1` |
| `FLOWFORGE_RESULT_ENQUEUE_BATCH_SIZE` | `1000` |
| `FLOWFORGE_RETRY_SCHEDULER_ENABLED` | `true` |
| `FLOWFORGE_RETRY_BATCH_SIZE` | `100` |
| `FLOWFORGE_RETRY_POLL_INTERVAL_MS` | `250` |
| `FLOWFORGE_TIMEOUT_REAPER_ENABLED` | `true` |
| `FLOWFORGE_TIMEOUT_BATCH_SIZE` | `100` |
| `FLOWFORGE_TIMEOUT_POLL_INTERVAL_MS` | `250` |
| `FLOWFORGE_WORKER_LEASE_DURATION` | `30s` |
| `FLOWFORGE_HEARTBEAT_CONSUMER_ENABLED` | `false`; production profile: `true` |
| `FLOWFORGE_HEARTBEAT_CONSUMER_GROUP` | `flowforge-control-plane-heartbeats-v1` |
| `FLOWFORGE_LEASE_REAPER_ENABLED` | `false`; production profile: `true` |
| `FLOWFORGE_LEASE_REAPER_BATCH_SIZE` | `100` |
| `FLOWFORGE_LEASE_REAPER_POLL_INTERVAL_MS` | `250` |
| `FLOWFORGE_SCHEDULING_ENABLED` | `false`; production profile: `true` |
| `FLOWFORGE_SCHEDULING_MATERIALIZATION_BATCH_SIZE` | `100` |
| `FLOWFORGE_SCHEDULING_PROCESSING_BATCH_SIZE` | `100` |
| `FLOWFORGE_SCHEDULING_POLL_INTERVAL_MS` | `1000` |
| `FLOWFORGE_SCHEDULING_LEASE_DURATION` | `30s` |
| `FLOWFORGE_SCHEDULING_RETRY_DELAY` | `5s` |
| `FLOWFORGE_SCHEDULING_MISFIRE_THRESHOLD` | `1m` |
| `FLOWFORGE_SCHEDULER_INSTANCE_ID` | Generated per process |
| `FLOWFORGE_COORDINATION_ENABLED` | `false`; production profile: `true` |
| `FLOWFORGE_ENVIRONMENT` | `local` |
| `FLOWFORGE_COORDINATION_LEASE_DURATION` | `30s` |
| `FLOWFORGE_COORDINATION_TTL_PADDING` | `2m` |
| `FLOWFORGE_CONCURRENCY_LEASE_DURATION` | `30s` |
| `FLOWFORGE_CONCURRENCY_RECONCILIATION_ENABLED` | `false`; production profile: `true` |
| `FLOWFORGE_CONCURRENCY_RECONCILIATION_BATCH_SIZE` | `200` |
| `FLOWFORGE_CONCURRENCY_RECONCILIATION_INTERVAL_MS` | `10000` |
| `FLOWFORGE_INSTANCE_ID` | Generated per process |
| `FLOWFORGE_WORKER_ID` | Generated per process |
| `FLOWFORGE_WORKER_GROUP` | `flowforge-workers-v1` |
| `FLOWFORGE_WORKER_DB_URL` | Falls back to `FLOWFORGE_DB_URL` |
| `FLOWFORGE_WORKER_DB_USERNAME` | Falls back to `FLOWFORGE_DB_USERNAME` |
| `FLOWFORGE_WORKER_DB_PASSWORD` | Falls back to `FLOWFORGE_DB_PASSWORD` |
| `FLOWFORGE_WORKER_DB_POOL_SIZE` | `10` |
| `FLOWFORGE_WORKER_CONCURRENCY` | `1` |
| `FLOWFORGE_WORKER_RESULT_BATCH_SIZE` | `100` |
| `FLOWFORGE_WORKER_RESULT_POLL_INTERVAL_MS` | `250` |
| `FLOWFORGE_WORKER_RESULT_LEASE_DURATION` | `30s` |
| `FLOWFORGE_WORKER_RESULT_PUBLISH_TIMEOUT` | `10s` |
| `FLOWFORGE_WORKER_HEARTBEAT_INTERVAL` | `10s` |
| `FLOWFORGE_KAFKA_HEALTH_TIMEOUT` | `3s` |
| `FLOWFORGE_WORKER_RESULT_PUBLISHER_ENABLED` | `true` |
| `PORT` | `8080` |
| `WORKER_PORT` | `8081` |

## Delivery and scaling

- Task commands are keyed by task execution ID so retries for one task stay ordered while unrelated tasks spread across worker partitions.
- Task results and execution events are keyed by workflow execution ID so one workflow's state changes remain ordered.
- Task heartbeats are keyed by task execution ID and can renew only the active attempt's fencing token.
- Worker and control-plane throughput scales with Kafka partitions; extra instances beyond the partition count provide standby capacity rather than additional active consumers.
- Delivery is at least once. Durable outboxes prevent loss, inbox keys suppress duplicates, and external task side effects must use the command event ID as their idempotency key.
- Known publish failures are released for retry. Unknown acknowledgement outcomes wait for the fenced claim lease to expire and may then produce a duplicate with the same event ID.
- Poison records are retried in place, then copied to a topic-specific V1 DLQ with source topic/partition/offset, raw key/value, failure metadata, and original correlation headers. A crash after DLQ acknowledgement but before source-offset commit may duplicate the deterministic source-record ID.
- `flowforge.kafka.dlq.record.age` exposes the recovery-age distribution and maximum observed age; publication, delivery-failure, recovery-failure, and recovered-record counters are tagged by bounded source-topic names.
- Scheduler replicas claim disjoint due definitions and pending triggers through PostgreSQL row locks. Trigger leases are token-fenced, and replay uses the same deterministic idempotency key, so a crash cannot create a second logical workflow execution.
- A stale recurring schedule creates at most one catch-up execution per poll. `SKIP` records the missed occurrence without execution; both policies advance directly to the first future cron occurrence to prevent catch-up storms.
- PostgreSQL serializes authoritative permit capacity per resource. Redis stores only an expiring, atomically maintained mirror; loss or eviction can reduce coordination efficiency but cannot grant capacity beyond the durable ledger.
- Concurrency limits belong to immutable workflow versions. Active PostgreSQL execution and attempt state is checked under the same resource lock as permit acquisition, so expired or missing Redis entries cannot oversubscribe a limit.

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/workflows` | Create workflow and draft version 1 |
| `GET` | `/api/v1/workflows` | List active workflows |
| `GET` | `/api/v1/workflows/{id}` | Read the current draft or latest published version |
| `PUT` | `/api/v1/workflows/{id}` | Update the draft; creates the next draft after publication |
| `POST` | `/api/v1/workflows/{id}/publish` | Publish the current draft |
| `DELETE` | `/api/v1/workflows/{id}` | Archive without deleting history |
| `POST` | `/api/v1/workflows/{id}/executions` | Start an idempotent execution of the latest published version |
| `GET` | `/api/v1/executions/{id}` | Inspect workflow state, tasks, attempts, and events |
| `POST` | `/api/v1/executions/{id}/cancel` | Request cancellation |
| `POST` | `/api/v1/schedules` | Create a one-time or cron schedule |
| `GET` | `/api/v1/schedules` | List schedules with optional workflow and status filters |
| `GET` | `/api/v1/schedules/{id}` | Inspect a schedule definition and its next fire time |
| `PUT` | `/api/v1/schedules/{id}` | Update a schedule using `If-Match` |
| `POST` | `/api/v1/schedules/{id}/pause` | Pause a schedule using `If-Match` |
| `POST` | `/api/v1/schedules/{id}/resume` | Resume a schedule using `If-Match` |
| `DELETE` | `/api/v1/schedules/{id}` | Soft-delete a schedule using `If-Match` |
| `GET` | `/actuator/health` | Health and availability probes |

Mutating an existing workflow requires the strong ETag returned by create/read/update:

```http
If-Match: "0"
```

Starting an execution requires an idempotency key. Reusing the same key for the same workflow returns the existing execution:

```http
Idempotency-Key: order-123
```

Example request:

```json
{
  "name": "Order processing",
  "description": "Validate, charge, and notify",
  "maxConcurrentExecutions": 20,
  "tasks": [
    {
      "key": "VALIDATE_ORDER",
      "name": "Validate order",
      "type": "NOOP",
      "configuration": {}
    },
    {
      "key": "PROCESS_PAYMENT",
      "name": "Process payment",
      "type": "DELAY",
      "configuration": {"durationMs": 100},
      "maxConcurrency": 5
    }
  ],
  "dependencies": [
    {
      "taskKey": "PROCESS_PAYMENT",
      "dependsOnTaskKey": "VALIDATE_ORDER"
    }
  ]
}
```

## Architecture

- `flowforge-domain`: pure Java definition invariants, execution state machines, DAG policy, and schedule models
- `flowforge-application`: workflow, execution, and scheduling use cases with outbound repository ports
- `flowforge-messaging`: versioned commands, results, events, and shared topic names
- `flowforge-kafka-support`: shared bounded-retry, broker-confirmed DLQ publication, metadata, and recovery metrics
- `flowforge-control-plane`: Spring Boot HTTP, PostgreSQL, transactional-outbox, and Kafka adapters
- `flowforge-worker`: independently deployable Spring Boot worker process

See the [project roadmap](docs/ROADMAP.md), [Phase 5 implementation plan](docs/PHASE_5_PLAN.md), [Phase 4 reliability runbook](docs/operations/phase-4-reliability-runbook.md), [ADR-001](docs/adr/001-phase-1-architecture.md), [ADR-002](docs/adr/002-phase-2-execution-architecture.md), [ADR-003](docs/adr/003-phase-3-distributed-execution.md), and [ADR-004](docs/adr/004-phase-4-reliability-and-recovery.md) for phase status, operations, and major design decisions.
