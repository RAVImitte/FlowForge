# FlowForge

FlowForge is a production-oriented distributed workflow and job orchestration platform. Phase 1 provides a clean-architecture foundation and a versioned workflow-definition control plane backed by PostgreSQL.

## Phase 1 capabilities

- Create, retrieve, list, update, publish, and archive workflow definitions
- Model task dependencies as a validated directed acyclic graph
- Preserve immutable published versions while allowing a new draft
- Prevent lost updates with strong ETags and `If-Match`
- Apply schema changes with Flyway
- Expose liveness, readiness, metrics, and application information
- Verify domain, HTTP, architecture, migration, and persistence behavior

Kafka execution, retries, scheduling, workers, and Redis coordination intentionally begin in later phases.

## Prerequisites

- Java 21
- Docker with Compose

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
| `GET` | `/actuator/health` | Health and availability probes |

Mutating an existing workflow requires the strong ETag returned by create/read/update:

```http
If-Match: "0"
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
      "type": "HTTP",
      "configuration": {"uri": "/payments"}
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

- `flowforge-domain`: pure Java invariants and DAG validation
- `flowforge-application`: use cases and outbound repository port
- `flowforge-control-plane`: Spring Boot HTTP and PostgreSQL adapters

See the [project roadmap](docs/ROADMAP.md) for phase status and [ADR-001](docs/adr/001-phase-1-architecture.md) for the major design decisions.
