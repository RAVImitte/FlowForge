# FlowForge

FlowForge is a production-oriented distributed workflow and job orchestration platform. Phase 3 adds Kafka-based delivery and independently scalable workers on top of Phase 2's durable PostgreSQL workflow engine.

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
- Provision versioned task-command, task-result, and execution-event topics
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

Phase 3 is complete. Retries, deadlines, DLQs, and worker-lease recovery are next in Phase 4; Redis coordination remains deliberately deferred to Phase 5.

## Prerequisites

- Java 21
- Rancher Desktop using the Moby engine, or another Docker-compatible runtime

The Maven wrapper downloads the pinned Maven version automatically.

## Run locally

Start PostgreSQL and Kafka:

```powershell
docker compose up -d postgres kafka
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
| `FLOWFORGE_DISPATCH_ENABLED` | `true`; production profile: `false` |
| `FLOWFORGE_DISPATCH_INTERVAL_MS` | `250` |
| `FLOWFORGE_KAFKA_ENABLED` | Control plane: `false`; production profile/worker: `true` |
| `FLOWFORGE_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `FLOWFORGE_KAFKA_TOPIC_PARTITIONS` | `6` |
| `FLOWFORGE_KAFKA_TOPIC_REPLICATION_FACTOR` | `1` |
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
| `FLOWFORGE_KAFKA_HEALTH_TIMEOUT` | `3s` |
| `FLOWFORGE_WORKER_RESULT_PUBLISHER_ENABLED` | `true` |
| `PORT` | `8080` |
| `WORKER_PORT` | `8081` |

## Delivery and scaling

- Task commands are keyed by task execution ID so retries for one task stay ordered while unrelated tasks spread across worker partitions.
- Task results and execution events are keyed by workflow execution ID so one workflow's state changes remain ordered.
- Worker and control-plane throughput scales with Kafka partitions; extra instances beyond the partition count provide standby capacity rather than additional active consumers.
- Delivery is at least once. Durable outboxes prevent loss, inbox keys suppress duplicates, and external task side effects must use the command event ID as their idempotency key.
- Known publish failures are released for retry. Unknown acknowledgement outcomes wait for the fenced claim lease to expire and may then produce a duplicate with the same event ID.

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
      "configuration": {"durationMs": 100}
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

- `flowforge-domain`: pure Java definition invariants, execution state machines, and DAG policy
- `flowforge-application`: use cases and outbound repository port
- `flowforge-messaging`: versioned commands, results, events, and shared topic names
- `flowforge-control-plane`: Spring Boot HTTP, PostgreSQL, transactional-outbox, and Kafka adapters
- `flowforge-worker`: independently deployable Spring Boot worker process

See the [project roadmap](docs/ROADMAP.md), [ADR-001](docs/adr/001-phase-1-architecture.md), [ADR-002](docs/adr/002-phase-2-execution-architecture.md), and [ADR-003](docs/adr/003-phase-3-distributed-execution.md) for phase status and major design decisions.
