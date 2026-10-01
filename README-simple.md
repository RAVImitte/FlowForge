# FlowForge

FlowForge is a distributed workflow and job orchestration platform. It lets you define a workflow as a directed acyclic graph (DAG) of tasks, publish an immutable version, start executions, and run the tasks reliably across independently scalable worker processes.

It is built for production-style operation: durable state, retries, idempotency, scheduling, tenant isolation, security, observability, and Kubernetes deployment are included.

## How it works

1. Use the control-plane API to create a workflow and publish a version.
2. Start an execution manually or through a one-time/cron schedule.
3. The control plane stores workflow state in PostgreSQL and sends ready tasks to Kafka.
4. Workers consume and execute tasks, then publish results.
5. The control plane applies results, unlocks dependent tasks, and records an execution event history.

PostgreSQL is the authoritative source of state. Kafka provides scalable, at-least-once task delivery; durable outbox/inbox records and idempotency keys make retries safe. Redis is a rebuildable coordination cache for permits and rate limiting.

## Main capabilities

- Workflow definitions with validated task dependencies and immutable published versions
- Idempotent workflow starts and task-result processing
- Task execution, retries, timeouts, cancellation, leases, and worker heartbeats
- One-time and cron schedules with time zones and misfire policies
- Concurrency limits, rate limits, back-pressure, and multi-instance-safe coordination
- Kafka dead-letter queues and safe replay tooling
- OAuth 2.0 JWT authentication, role-based access, tenant quotas, audit records, and secret references
- Prometheus metrics, OpenTelemetry tracing, structured logs, Grafana dashboards, and alerts
- Docker images and Helm deployment assets for Kubernetes

## Project layout

| Module | Purpose |
|---|---|
| `flowforge-domain` | Core workflow rules, DAG validation, and execution state models |
| `flowforge-application` | Workflow, execution, and scheduling use cases |
| `flowforge-messaging` | Versioned Kafka commands, results, events, and topic definitions |
| `flowforge-kafka-support` | Kafka retries, dead-letter handling, and recovery metrics |
| `flowforge-control-plane` | Spring Boot API, PostgreSQL persistence, scheduling, and Kafka integration |
| `flowforge-worker` | Independently deployable task-execution service |
| `flowforge-observability` | Shared logging, metrics, and tracing support |
| `flowforge-load-test` | Load generator and smoke/soak test tooling |

## Requirements

- Java 21
- Rancher Desktop with the Moby engine, Docker Desktop, or another Docker-compatible runtime

The Maven wrapper downloads the required Maven version automatically.

## Quick start

Start the local dependencies:

```powershell
docker compose up -d postgres kafka redis
```

Run the control plane in the production-style Kafka topology. Disabling security below is for local development only:

```powershell
$env:JAVA_HOME = 'C:\path\to\jdk-21'
$env:SPRING_PROFILES_ACTIVE = 'production'
$env:FLOWFORGE_SECURITY_ENABLED = 'false'
.\mvnw.cmd -pl flowforge-control-plane -am spring-boot:run
```

In a second terminal, run a worker:

```powershell
$env:SPRING_PROFILES_ACTIVE = 'production'
.\mvnw.cmd -pl flowforge-worker -am spring-boot:run
```

Run the test suite:

```powershell
.\mvnw.cmd verify
```

## Useful endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/v1/workflows` | Create a workflow |
| `POST` | `/api/v1/workflows/{id}/publish` | Publish its current draft |
| `POST` | `/api/v1/workflows/{id}/executions` | Start an execution; send `Idempotency-Key` |
| `GET` | `/api/v1/executions/{id}` | Inspect execution, task, attempt, and event state |
| `POST` | `/api/v1/executions/{id}/cancel` | Cancel an execution |
| `POST` | `/api/v1/schedules` | Create a one-time or cron schedule |
| `GET` | `/actuator/health` | Check service health |

Workflow updates and schedule mutations use the strong ETag supplied by the API in an `If-Match` header.

## Optional local observability

```powershell
docker compose --profile observability up -d prometheus grafana otel-collector
```

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

Set `FLOWFORGE_OTLP_ENABLED=true` before starting an application if you want it to export sampled traces to the local collector.

## Configuration

The usual local defaults are:

| Variable | Default |
|---|---|
| `FLOWFORGE_DB_URL` | `jdbc:postgresql://localhost:5432/flowforge` |
| `FLOWFORGE_DB_USERNAME` / `FLOWFORGE_DB_PASSWORD` | `flowforge` / `flowforge` |
| `FLOWFORGE_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `FLOWFORGE_REDIS_HOST` | `localhost` |
| `FLOWFORGE_REDIS_PORT` | `6379` |
| `PORT` / `WORKER_PORT` | `8080` / `8081` |

For authenticated deployments, keep security enabled and configure `FLOWFORGE_OIDC_ISSUER_URI` and `FLOWFORGE_OIDC_JWK_SET_URI`.

## More documentation

- [Original detailed README](README.md)
- [Project roadmap](docs/ROADMAP.md)
- [Helm deployment guide](deploy/helm/flowforge/README.md)
- [Load-testing guide](load-testing/README.md)
- [Architecture decisions](docs/adr/)
- [Operations runbooks](docs/operations/)
- [Security policy](docs/security/secret-and-audit-policy.md)
