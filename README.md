# FlowForge

FlowForge is a production-oriented distributed workflow and job orchestration platform. Phases 1-6 establish durable, observable distributed execution; Phase 7 adds security and operational release readiness.

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
- Enforce PostgreSQL-authoritative token buckets across scheduler and task-dispatch replicas
- Mirror versioned token-bucket state into TTL-bounded Redis keys and rebuild it after key loss
- Bound pending schedule triggers and per-workflow ready-task queues before resource exhaustion
- Delay schedule fires and task claims when rate capacity is exhausted without losing durable work
- Return HTTP 429 overload responses with explicit `Retry-After` guidance
- Expose saturation, throttling, queue-depth, queue-age, and rejected-admission metrics
- Rebalance Kafka consumers cooperatively while allowing bounded in-flight handlers to finish during graceful shutdown
- Recover an abandoned schedule through control-plane and worker replacement while reconstructing active permit mirrors after total Redis key loss
- Operate scheduling and coordination through a dedicated runbook and an accepted architecture decision record
- Scope consistent workflow, task, attempt, event, fencing, and Kafka metadata into leak-safe structured log context
- Accept safe `X-Correlation-Id` request identifiers, generate replacements for unsafe values, and echo the effective ID
- Emit ECS JSON console logs in production profiles while retaining readable local and test output
- Trace HTTP, Kafka, scheduled workflow processing, and worker execution with Micrometer and OpenTelemetry
- Preserve bounded W3C trace and baggage context across PostgreSQL-backed outbox delays and process restarts
- Keep OTLP export opt-in and independent from workflow transactions and Kafka acknowledgements
- Expose Prometheus-format application, JVM, HTTP, workflow-throughput, backlog-age, scheduling, and saturation metrics
- Provision a versioned Grafana operations dashboard and local Prometheus/OpenTelemetry Collector stack
- Evaluate API-availability and workflow-start error budgets with multi-window burn-rate alerts
- Alert on durable queue/completion age, outbox stalls, retries, timeouts, dead letters, permit leaks, admission pressure, and dependency loss with linked runbooks
- Authenticate production API requests as issuer-validated OAuth 2.0 JWT bearer tokens
- Enforce deny-by-default `VIEWER`, `OPERATOR`, `ADMIN`, and `MONITOR` role boundaries
- Keep health probes public while protecting metrics and administrative actuator endpoints
- Return stable problem responses for missing credentials and forbidden operations
- Derive bounded tenant identity from validated JWT claims while ignoring spoofable tenant headers
- Backfill durable workflow, execution, and schedule ownership through a versioned tenant-registry migration
- Persist optimistic-versioned tenant quotas for executions, tasks, schedules, ready queues, and admission rates
- Enforce tenant capacity through PostgreSQL-authoritative locks across control-plane replicas while treating Redis as a recoverable mirror
- Administer the authenticated tenant's quota with ETags and stale-writer fencing
- Reject inline task secrets and carry provider-neutral secret references through PostgreSQL and Kafka without resolved material
- Resolve secret references only inside workers and redact sensitive handler failures before logs, traces, events, or durable details
- Record fail-closed, append-only administrative audit attempt/outcome pairs with tenant, actor, action, target, and request identity
- Query tenant-scoped audit history through an administrator-only API with a configurable online retention window
- Build provenance-enabled control-plane and worker images from digest-pinned Java 21 bases with deterministic artifact timestamps
- Run both images as UID/GID 10001 with health checks and no writable application directory
- Deploy independently scalable control-plane and worker workloads through a versioned Helm chart
- Protect Kubernetes rollouts with startup/liveness/readiness probes, graceful drains, disruption budgets, resources, and topology spread
- Enforce restricted pod/container security contexts and source database, Kafka, Redis, OIDC, and trust-store material only from external Secrets
- Gate pull requests and releases with architecture, security/tenant-isolation, upgrade/rollback, recovery, distributed-integration, manifest, SBOM, vulnerability, smoke, provenance, signing, and attestation checks

Phases 1-7 are complete. The final same-revision acceptance matrix proves security and tenant isolation, populated-schema upgrade and rollback compatibility, logical restore, named-point WAL/PITR, Redis reconstruction, bounded tenant-safe DLQ replay, the full distributed reactor, deployment manifests, and aggregate SBOM generation. Publication remains an authorized action through the protected release workflow.

## Prerequisites

- Java 21
- Rancher Desktop using the Moby engine, or another Docker-compatible runtime

The Maven wrapper downloads the pinned Maven version automatically.

## Build images and render a deployment

Build and inspect both Linux images through the Docker-compatible engine:

```powershell
.\scripts\build-images.ps1 -Registry flowforge -Tag local
```

The builder accepts the active JDK truststore as an ephemeral BuildKit secret so
corporate TLS trust can be used without entering the output image. Production
credentials and runtime trust are supplied only through Kubernetes Secret
references. Lint and render the deployment contract with:

```powershell
helm lint .\deploy\helm\flowforge --strict
helm template flowforge .\deploy\helm\flowforge --namespace flowforge
```

See the [Helm deployment guide](deploy/helm/flowforge/README.md) for the required
external Secret contract, digest-pinned installation, autoscaling constraints,
rollout verification, and rollback commands.

## Run locally

Start PostgreSQL, Kafka, and Redis:

```powershell
docker compose up -d postgres kafka redis
```

Run the control plane in its production Kafka topology:

```powershell
$env:JAVA_HOME = 'C:\path\to\jdk-21'
$env:SPRING_PROFILES_ACTIVE = 'production'
$env:FLOWFORGE_SECURITY_ENABLED = 'false' # local development only
.\mvnw.cmd -pl flowforge-control-plane -am spring-boot:run
```

The `production` profile enables Kafka, outbox publication, command dispatch, result consumption, and JWT security, and disables the Phase 2 in-process dispatcher. Startup validation rejects incomplete execution topologies and missing security configuration. The example explicitly disables authentication only for a local topology without an identity provider.

For an authenticated deployment, leave security enabled and configure the trusted issuer and its JWK endpoint:

```powershell
$env:FLOWFORGE_OIDC_ISSUER_URI = 'https://identity.example.com/realms/flowforge'
$env:FLOWFORGE_OIDC_JWK_SET_URI = 'https://identity.example.com/realms/flowforge/protocol/openid-connect/certs'
```

Run a worker in a second terminal (it uses the same local PostgreSQL service by default, with an independent Flyway history table):

```powershell
$env:SPRING_PROFILES_ACTIVE = 'production'
.\mvnw.cmd -pl flowforge-worker -am spring-boot:run
```

Start the versioned local observability stack:

```powershell
docker compose --profile observability up -d prometheus grafana otel-collector
```

Prometheus is available at `http://localhost:9090` and the provisioned FlowForge dashboard at `http://localhost:3000`. The services scrape the control plane and worker from ports `8080` and `8081`. To send sampled traces to the local collector, set `$env:FLOWFORGE_OTLP_ENABLED = 'true'` before starting each application; trace export remains optional and metrics continue to use Prometheus pull semantics.

Validate Prometheus syntax, alert behavior, and the Compose profile with the pinned image:

```powershell
.\scripts\verify-observability.ps1
```

Run all tests:

```powershell
.\mvnw.cmd verify
```

Run the bounded distributed smoke workload after starting the production topology:

```powershell
.\scripts\run-load-test.ps1 -Profile smoke
```

Or let the managed runner build, start, measure, reconcile, and clean up a named topology:

```powershell
.\scripts\run-load-topology.ps1 -Topology balanced-2x2-6p -Profile smoke
```

The [load-testing guide](load-testing/README.md) documents the smoke, overload, soak, and scheduled profiles, JSON report contract, and threshold exit behavior.

Infrastructure integration tests use PostgreSQL, Kafka, and Redis Testcontainers. They are skipped when a Docker-compatible runtime is unavailable; all other tests still run.

Operational procedures are in the [Phase 4 reliability runbook](docs/operations/phase-4-reliability-runbook.md), [Phase 5 scheduling and coordination runbook](docs/operations/phase-5-scheduling-coordination-runbook.md), [Phase 6 SLO and alerting runbook](docs/operations/phase-6-slo-alerting-runbook.md), and [Phase 6 observability/capacity/resilience runbook](docs/operations/phase-6-observability-capacity-resilience-runbook.md). Deployment and rollback are documented in the [Helm deployment guide](deploy/helm/flowforge/README.md). Secret handling, redaction, audit, and retention are defined in the [security policy](docs/security/secret-and-audit-policy.md). The final telemetry and scaling design is recorded in [ADR-006](docs/adr/006-observability-scalability-and-resilience.md).

## Configuration

| Environment variable | Default |
|---|---|
| `FLOWFORGE_DB_URL` | `jdbc:postgresql://localhost:5432/flowforge` |
| `FLOWFORGE_DB_USERNAME` | `flowforge` |
| `FLOWFORGE_DB_PASSWORD` | `flowforge` |
| `FLOWFORGE_DB_POOL_SIZE` | `10` |
| `FLOWFORGE_SHUTDOWN_TIMEOUT` | Control plane: `30s`; worker: `70s` |
| `FLOWFORGE_LOG_FORMAT` | Production profiles: `ecs` |
| `FLOWFORGE_TRACING_SAMPLING_PROBABILITY` | `0.1` |
| `FLOWFORGE_OTLP_ENABLED` | `false` |
| `FLOWFORGE_OTLP_TRACES_ENDPOINT` | `http://localhost:4318/v1/traces` |
| `FLOWFORGE_PROMETHEUS_ENABLED` | `true` |
| `FLOWFORGE_SECURITY_ENABLED` | `false`; production profile: `true` |
| `FLOWFORGE_OIDC_ISSUER_URI` | Required when security is enabled |
| `FLOWFORGE_OIDC_JWK_SET_URI` | Required when security is enabled |
| `FLOWFORGE_SECURITY_ROLES_CLAIM` | `roles` |
| `FLOWFORGE_SECURITY_TENANT_ID_CLAIM` | `tenant_id` |
| `FLOWFORGE_LOCAL_TENANT_ID` | `local`; used only when security is disabled |
| `FLOWFORGE_AUDIT_ONLINE_RETENTION` | `365d` |
| `FLOWFORGE_TENANT_MAX_ACTIVE_EXECUTIONS` | `10000`; inherited when no tenant override exists |
| `FLOWFORGE_TENANT_MAX_RUNNING_TASKS` | `10000`; inherited when no tenant override exists |
| `FLOWFORGE_TENANT_MAX_READY_TASKS` | `10000`; inherited when no tenant override exists |
| `FLOWFORGE_TRACE_MAX_ATTRIBUTES` | `64` |
| `FLOWFORGE_TRACE_MAX_ATTRIBUTE_VALUE_LENGTH` | `1024` |
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
| `FLOWFORGE_DLQ_REPLAY_TRANSPORT_TIMEOUT` | `10s` |
| `FLOWFORGE_DLQ_REPLAY_CLAIM_LEASE` | `30s` |
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
| `FLOWFORGE_SCHEDULE_RATE_CAPACITY` | `100` |
| `FLOWFORGE_SCHEDULE_RATE_REFILL_TOKENS` | `100` |
| `FLOWFORGE_SCHEDULE_RATE_REFILL_PERIOD` | `1s` |
| `FLOWFORGE_DISPATCH_RATE_CAPACITY` | `200` |
| `FLOWFORGE_DISPATCH_RATE_REFILL_TOKENS` | `200` |
| `FLOWFORGE_DISPATCH_RATE_REFILL_PERIOD` | `1s` |
| `FLOWFORGE_RATE_LIMIT_MIRROR_TTL` | `10m` |
| `FLOWFORGE_MAX_PENDING_SCHEDULE_FIRES` | `10000` |
| `FLOWFORGE_MAX_READY_TASKS_PER_WORKFLOW` | `1000` |
| `FLOWFORGE_ADMISSION_RETRY_AFTER` | `1s` |
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
| `GET` | `/api/v1/tenant/quota` | Read the authenticated tenant's configured or inherited quota |
| `PUT` | `/api/v1/tenant/quota` | Update the authenticated tenant's quota using `If-Match` |
| `GET` | `/api/v1/audit-events` | Read paginated tenant audit history; `ADMIN` only |
| `GET` | `/api/v1/dead-letters/{topic}/partitions/{partition}/offsets/{offset}` | Inspect payload-free metadata and digest for one tenant-owned DLQ record; `ADMIN` only |
| `POST` | `/api/v1/dead-letters/{topic}/partitions/{partition}/offsets/{offset}/replay` | Idempotently replay one tenant-owned record with an `Idempotency-Key`; `ADMIN` only |
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
      "secretReferences": {
        "apiKey": {"provider": "env", "name": "PAYMENT_API_KEY"}
      },
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
- `flowforge-load-test`: packaged Java 21 fan-out/fan-in load generator with thresholded JSON reports

See the [project roadmap](docs/ROADMAP.md), [Phase 7 implementation plan](docs/PHASE_7_PLAN.md), [release acceptance matrix](docs/operations/phase-7-release-acceptance.md), [release pipeline guide](docs/operations/release-pipeline.md), [Phase 7 recovery runbook](docs/operations/phase-7-recovery-runbook.md), [incident and DLQ replay runbook](docs/operations/phase-7-incident-and-dlq-replay-runbook.md), [upgrade and credential-rotation runbook](docs/operations/phase-7-upgrade-and-credential-rotation-runbook.md), [Phase 6 operations runbook](docs/operations/phase-6-observability-capacity-resilience-runbook.md), [resilience evidence](docs/resilience/README.md), and [architecture decisions](docs/adr/) for phase status, operations, and major design decisions.
