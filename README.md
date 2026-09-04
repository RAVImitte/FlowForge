# FlowForge

FlowForge is a production-oriented distributed workflow and job orchestration platform. Phase 2 provides durable DAG execution, explicit state machines, idempotent workflow starts, concurrency-safe task coordination, and execution history backed by PostgreSQL.

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
- Execute deterministic `NOOP`, `DELAY`, and `FAIL` handlers in process

Kafka delivery, distributed workers, retries, deadlines, DLQs, and Redis coordination intentionally begin in later phases.

## Prerequisites

- Java 21
- Rancher Desktop using the Moby engine, or another Docker-compatible runtime

The Maven wrapper downloads the pinned Maven version automatically.

## Run locally

Start PostgreSQL:

```powershell
docker compose up -d postgres
```

Run the control plane:

```powershell
$env:JAVA_HOME = 'C:\path\to\jdk-21'
.\mvnw.cmd -pl flowforge-control-plane -am spring-boot:run
```

Run all tests:

```powershell
.\mvnw.cmd verify
```

Tests that require PostgreSQL use Testcontainers. They are skipped when Docker is not available; all other tests still run.

## Configuration

| Environment variable | Default |
|---|---|
| `FLOWFORGE_DB_URL` | `jdbc:postgresql://localhost:5432/flowforge` |
| `FLOWFORGE_DB_USERNAME` | `flowforge` |
| `FLOWFORGE_DB_PASSWORD` | `flowforge` |
| `FLOWFORGE_DB_POOL_SIZE` | `10` |
| `FLOWFORGE_DISPATCH_ENABLED` | `true` |
| `FLOWFORGE_DISPATCH_INTERVAL_MS` | `250` |
| `PORT` | `8080` |

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
- `flowforge-control-plane`: Spring Boot HTTP and PostgreSQL adapters

See the [project roadmap](docs/ROADMAP.md), [ADR-001](docs/adr/001-phase-1-architecture.md), and [ADR-002](docs/adr/002-phase-2-execution-architecture.md) for phase status and major design decisions.
