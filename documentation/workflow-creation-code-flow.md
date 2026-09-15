# Workflow Creation — Complete Code Flow

> A detailed walkthrough of every function called when `POST /api/v1/workflows` is invoked, from HTTP request to database persistence and back.

---

## Table of Contents

1. [HTTP Request Arrives](#1-http-request-arrives)
2. [TenantContextFilter — Extract Tenant Identity](#2-tenantcontextfilter--extract-tenant-identity)
3. [WorkflowController.create() — Inbound HTTP Adapter](#3-workflowcontrollercreate--inbound-http-adapter)
4. [WorkflowController.toDraft() — DTO → Domain Conversion](#4-workflowcontrollertodraft--dto--domain-conversion)
5. [WorkflowDraft Constructor — Domain Validation (DAG + Invariants)](#5-workflowdraft-constructor--domain-validation-dag--invariants)
6. [WorkflowService.create() — Application Service](#6-workflowservicecreate--application-service)
7. [JdbcWorkflowRepository.create() — Persistence Adapter](#7-jdbcworkflowrepositorycreate--persistence-adapter)
8. [WorkflowResponse.from() — Domain → HTTP DTO](#8-workflowresponsefrom--domain--http-dto)
9. [HTTP Response](#9-http-response)
10. [Summary: Complete Call Chain](#10-summary-complete-call-chain)
11. [Why Each Layer Exists (Clean Architecture)](#11-why-each-layer-exists-clean-architecture)

---

## 1. HTTP Request Arrives

```
Client sends:
  POST /api/v1/workflows
  Content-Type: application/json
  Authorization: Bearer <JWT>

  {
    "name": "Order processing",
    "description": "Validate, charge, and notify",
    "tasks": [
      {"key": "VALIDATE_ORDER", "name": "Validate order", "type": "NOOP", "configuration": {}},
      {"key": "PROCESS_PAYMENT", "name": "Process payment", "type": "HTTP", "configuration": {"uri": "/payments"}}
    ],
    "dependencies": [
      {"taskKey": "PROCESS_PAYMENT", "dependsOnTaskKey": "VALIDATE_ORDER"}
    ]
  }
```

---

## 2. TenantContextFilter — Extract Tenant Identity

**File:** `flowforge-control-plane/src/main/java/io/flowforge/controlplane/config/TenantContextFilter.java`

Before the controller method runs, a servlet filter intercepts the request. It extracts the tenant ID from the JWT bearer token (validated against the configured OAuth 2.0 issuer and JWK set). The tenant ID is stored in a thread-local `TenantContext`.

**Why needed:** Multi-tenancy is a core requirement (ADR-007). Every query in the system is tenant-scoped — a workflow created by tenant A must never be visible to tenant B. The filter ensures the tenant identity is established before any business logic runs.

---

## 3. WorkflowController.create() — Inbound HTTP Adapter

**File:** `flowforge-control-plane/src/main/java/io/flowforge/controlplane/adapter/in/web/WorkflowController.java` (line 38–49)

```java
@PostMapping
ResponseEntity<WorkflowResponse> create(
        HttpServletRequest httpRequest,
        @Valid @RequestBody WorkflowRequest request
) {
    TenantId tenantId = TenantContextFilter.requireTenant(httpRequest);
    WorkflowDefinition created = service.create(tenantId, toDraft(request));
    return ResponseEntity
            .created(URI.create("/api/v1/workflows/" + created.id()))
            .eTag(etag(created.lockVersion()))
            .body(WorkflowResponse.from(created));
}
```

**What happens:**

1. `@Valid @RequestBody WorkflowRequest request` — Spring's Bean Validation runs first. It checks `@NotBlank`, `@Size`, `@Pattern`, `@NotEmpty` annotations on `WorkflowRequest`. If any constraint fails, Spring throws `MethodArgumentNotValidException` → mapped to `400 Bad Request` by `ApiExceptionHandler`.
2. `TenantContextFilter.requireTenant(httpRequest)` — Retrieves the tenant ID from the request context. Throws if missing.
3. `toDraft(request)` — Converts the HTTP DTO to a domain object (see Step 4).
4. `service.create(tenantId, draft)` — Delegates to the application service (see Step 5).
5. Builds the HTTP response: `201 Created` with `Location` header and `ETag` header.

**Why needed:** This is the hexagonal "inbound adapter" — it translates HTTP concerns (JSON, headers, status codes) into domain calls. The controller knows about HTTP; the domain does not.

---

## 4. WorkflowController.toDraft() — DTO → Domain Conversion

**File:** `WorkflowController.java` (line 112–143)

```java
private static WorkflowDraft toDraft(WorkflowRequest request) {
    List<TaskDefinition> tasks = request.tasks().stream()
            .map(task -> new TaskDefinition(
                    task.key(), task.name(), task.type(), task.configuration(),
                    /* secretReferences */, /* reliabilityPolicy */, task.maxConcurrency()
            ))
            .toList();
    List<TaskDependency> dependencies = request.dependencies() == null
            ? List.of()
            : request.dependencies().stream()
                    .map(dependency -> new TaskDependency(
                            dependency.taskKey(), dependency.dependsOnTaskKey()
                    ))
                    .toList();
    return new WorkflowDraft(
            request.name(), request.description(), tasks, dependencies, request.maxConcurrentExecutions()
    );
}
```

**What happens:**

- Maps each `TaskRequest` (HTTP DTO) → `TaskDefinition` (domain record), including:
  - `secretReferences` — converts `SecretReferenceRequest` → `SecretReference` domain objects (provider, name, version). These are *references* to secrets, never resolved values.
  - `reliabilityPolicy` — converts `ReliabilityPolicyRequest` → `TaskReliabilityPolicy` domain object (maxAttempts, initialBackoff, backoffMultiplier, maxBackoff, jitterFactor, attemptTimeout, retryableErrorCodes). Defaults: maxAttempts=1, backoffMultiplier=2.0, jitter=0.0.
- Maps each `DependencyRequest` → `TaskDependency` (taskKey, dependsOnTaskKey).
- Constructs `new WorkflowDraft(...)` — **this is where DAG validation runs** (see Step 5).

**Why needed:** The domain model must not depend on Jakarta validation annotations or HTTP JSON structures. This conversion keeps the domain pure while allowing rich HTTP DTOs with validation.

---

## 5. WorkflowDraft Constructor — Domain Validation (DAG + Invariants)

**File:** `flowforge-domain/src/main/java/io/flowforge/domain/workflow/WorkflowDraft.java` (line 22–103)

The `WorkflowDraft` record's compact constructor calls `validate()`, which performs **all-collecting validation**:

1. **Name checks** — not blank, ≤ 200 chars
2. **Description checks** — ≤ 2,000 chars
3. **Task list checks** — at least 1 task, at most 1,000 tasks
4. **Per-task checks** — key matches `[A-Z][A-Z0-9_]{0,99}`, name not blank ≤ 200 chars, type matches `[A-Z][A-Z0-9_.-]{0,99}`
5. **Duplicate task keys** — rejected via `Set.add()` returning false
6. **Dependency checks** — both `taskKey` and `dependsOnTaskKey` must reference existing tasks; no self-dependencies; no duplicate edges
7. **Cycle detection** — Kahn's topological sort algorithm (see below)

### Kahn's Algorithm (`containsCycle()`, line 111–137)

```
1. Build in-degree map: for each dependency (A depends on B), increment in-degree[A]
2. Build dependents adjacency list: dependents[B].add(A)
3. Queue all nodes with in-degree 0
4. While queue not empty:
   a. Dequeue a node, increment visited count
   b. For each dependent of that node, decrement its in-degree
   c. If in-degree reaches 0, add to queue
5. If visited ≠ total nodes → cycle exists
```

If any violations are found, **all** are collected and thrown together as `DomainValidationException` with a `List<String>` of violation messages. This is **not** fail-fast — the caller gets every problem in one response.

**Why needed:** The domain is the single source of truth for invariants. If a cyclic graph were persisted, execution would deadlock. By validating at construction time, it's impossible to create an invalid `WorkflowDraft` object — the type system guarantees correctness.

---

## 6. WorkflowService.create() — Application Service

**File:** `flowforge-application/src/main/java/io/flowforge/application/workflow/WorkflowService.java` (line 17–19)

```java
public WorkflowDefinition create(TenantId tenantId, WorkflowDraft draft) {
    return repository.create(Objects.requireNonNull(tenantId), draft);
}
```

**What happens:** Validates that `tenantId` is not null, then delegates directly to the repository.

**Why needed (even though it's thin):**

- It's the **use-case boundary** — the place where future cross-cutting concerns (authorization checks, audit logging, domain event dispatch) would live.
- It validates the `tenantId` is present — the repository trusts the service to provide it.
- It keeps the controller free of repository knowledge — the controller depends on the service, not the repository.

---

## 7. JdbcWorkflowRepository.create() — Persistence Adapter

**File:** `flowforge-control-plane/src/main/java/io/flowforge/controlplane/adapter/out/persistence/JdbcWorkflowRepository.java` (line 60–89)

```java
@Override
@Transactional
public WorkflowDefinition create(TenantId tenantId, WorkflowDraft draft) {
    UUID workflowId = UUID.randomUUID();
    UUID versionId = UUID.randomUUID();

    // 7a. Insert workflow_definition
    jdbc.sql("""
            INSERT INTO workflow_definition(id, tenant_id, lifecycle_status)
            VALUES (:id, :tenantId, 'ACTIVE')
            """)
            .param("id", workflowId)
            .param("tenantId", tenantId.value())
            .update();

    // 7b. Insert workflow_version (DRAFT, version 1)
    jdbc.sql("""
            INSERT INTO workflow_version(
                id, workflow_id, version_number, version_status, name, description,
                max_concurrent_executions
            ) VALUES (
                :id, :workflowId, 1, 'DRAFT', :name, :description,
                :maxConcurrentExecutions
            )
            """)
            .param("id", versionId)
            .param("workflowId", workflowId)
            .param("name", draft.name())
            .param("description", draft.description())
            .param("maxConcurrentExecutions", draft.maxConcurrentExecutions())
            .update();

    // 7c. Insert tasks and dependencies
    replaceGraph(versionId, draft);

    // 7d. Read back the full workflow
    return findById(tenantId, workflowId).orElseThrow();
}
```

### 7a. Insert `workflow_definition` row

- Generates a random UUID for the workflow ID
- Inserts with `lifecycle_status = 'ACTIVE'`, `lock_version = 0` (database default), `tenant_id` from the request
- The `tenant_id` column enforces tenant isolation at the database level

### 7b. Insert `workflow_version` row

- Generates a random UUID for the version ID
- Inserts version 1 with `version_status = 'DRAFT'`
- The partial unique index `uq_workflow_single_draft` on `(workflow_id) WHERE version_status = 'DRAFT'` guarantees at most one draft per workflow

### 7c. `replaceGraph(versionId, draft)` — Insert tasks and dependencies

**File:** `JdbcWorkflowRepository.java` (line 302–355)

```java
private void replaceGraph(UUID versionId, WorkflowDraft draft) {
    // Delete existing tasks and dependencies (none on create, but used on update)
    jdbc.sql("DELETE FROM workflow_dependency WHERE workflow_version_id = :versionId")
            .param("versionId", versionId).update();
    jdbc.sql("DELETE FROM workflow_task WHERE workflow_version_id = :versionId")
            .param("versionId", versionId).update();

    // Insert each task with position ordering
    for (int position = 0; position < draft.tasks().size(); position++) {
        TaskDefinition task = draft.tasks().get(position);
        jdbc.sql("""
                INSERT INTO workflow_task(
                    id, workflow_version_id, task_key, task_name, task_type, configuration,
                    secret_references, position,
                    max_attempts, initial_backoff_ms, backoff_multiplier, max_backoff_ms,
                    jitter_factor, attempt_timeout_ms, retryable_error_codes, max_concurrency
                ) VALUES (
                    :id, :versionId, :taskKey, :taskName, :taskType,
                    CAST(:configuration AS jsonb), CAST(:secretReferences AS jsonb), :position,
                    :maxAttempts, :initialBackoffMs, :backoffMultiplier, :maxBackoffMs,
                    :jitterFactor, :attemptTimeoutMs, CAST(:retryableErrorCodes AS jsonb),
                    :maxConcurrency
                )
                """)
                .param("id", UUID.randomUUID())
                .param("versionId", versionId)
                .param("taskKey", task.key())
                .param("taskName", task.name())
                .param("taskType", task.type())
                .param("configuration", toJson(task.configuration()))
                .param("secretReferences", toJson(task.secretReferences()))
                .param("position", position)
                .param("maxAttempts", task.reliabilityPolicy().maxAttempts())
                .param("initialBackoffMs", task.reliabilityPolicy().initialBackoff().toMillis())
                .param("backoffMultiplier", task.reliabilityPolicy().backoffMultiplier())
                .param("maxBackoffMs", task.reliabilityPolicy().maxBackoff().toMillis())
                .param("jitterFactor", task.reliabilityPolicy().jitterFactor())
                .param("attemptTimeoutMs", task.reliabilityPolicy().attemptTimeout() == null
                        ? null
                        : task.reliabilityPolicy().attemptTimeout().toMillis())
                .param("retryableErrorCodes", toJson(task.reliabilityPolicy().retryableErrorCodes()))
                .param("maxConcurrency", task.maxConcurrency())
                .update();
    }

    // Insert each dependency edge
    for (TaskDependency dependency : draft.dependencies()) {
        jdbc.sql("""
                INSERT INTO workflow_dependency(
                    workflow_version_id, task_key, depends_on_task_key
                ) VALUES (:versionId, :taskKey, :dependsOnTaskKey)
                """)
                .param("versionId", versionId)
                .param("taskKey", dependency.taskKey())
                .param("dependsOnTaskKey", dependency.dependsOnTaskKey())
                .update();
    }
}
```

- `configuration` and `secretReferences` are serialized to JSON via Jackson's `ObjectMapper.writeValueAsString()`, then cast to `jsonb` by PostgreSQL
- `retryableErrorCodes` (a `Set<String>`) is also serialized to JSONB
- The `position` column preserves task ordering
- Foreign keys on `workflow_dependency` reference `workflow_task(workflow_version_id, task_key)`, ensuring no dangling dependencies at the database level

### 7d. `findById(tenantId, workflowId)` — Read back the full workflow

**File:** `JdbcWorkflowRepository.java` (line 91–117)

```java
public Optional<WorkflowDefinition> findById(TenantId tenantId, UUID id) {
    return jdbc.sql(SELECT_CURRENT)
            .param("id", id)
            .param("tenantId", tenantId.value())
            .query((rs, rowNum) -> {
                UUID versionId = rs.getObject("workflow_version_id", UUID.class);
                return new WorkflowDefinition(
                        rs.getObject("id", UUID.class),
                        new TenantId(rs.getString("tenant_id")),
                        rs.getLong("lock_version"),
                        WorkflowLifecycleStatus.valueOf(rs.getString("lifecycle_status")),
                        rs.getInt("version_number"),
                        WorkflowVersionStatus.valueOf(rs.getString("version_status")),
                        rs.getString("name"),
                        rs.getString("description"),
                        (Integer) rs.getObject("max_concurrent_executions"),
                        loadTasks(versionId),      // separate query
                        loadDependencies(versionId), // separate query
                        instant(rs.getObject("created_at")),
                        instant(rs.getObject("updated_at")),
                        instant(rs.getObject("published_at"))
                );
            })
            .optional();
}
```

The `SELECT_CURRENT` query uses a `LATERAL` join to pick the "current" version:

```sql
SELECT w.*, v.*
  FROM workflow_definition w
  JOIN LATERAL (
    SELECT candidate.*
      FROM workflow_version candidate
     WHERE candidate.workflow_id = w.id
     ORDER BY CASE WHEN candidate.version_status = 'DRAFT' THEN 0 ELSE 1 END,
              candidate.version_number DESC
     LIMIT 1
  ) v ON TRUE
 WHERE w.id = :id
   AND w.tenant_id = :tenantId
   AND w.lifecycle_status = 'ACTIVE'
```

This picks the DRAFT if one exists (priority 0), otherwise the latest PUBLISHED (priority 1, descending version number). Then `loadTasks()` and `loadDependencies()` run separate queries to fetch the task graph.

**Why `@Transactional`:** All 4 sub-steps (insert definition, insert version, insert tasks, insert dependencies) must succeed or fail atomically. If the task insert fails, the definition and version inserts must roll back. The `@Transactional` annotation ensures this.

**Why read back at the end:** The `create()` method returns a `WorkflowDefinition` (the full domain object with all metadata, tasks, dependencies, timestamps). Rather than constructing it manually from the inputs, it reads back from the database — this ensures the returned object matches exactly what's persisted, including database-generated defaults (`lock_version = 0`, `created_at`, `updated_at`).

---

## 8. WorkflowResponse.from() — Domain → HTTP DTO

**File:** `flowforge-control-plane/src/main/java/io/flowforge/controlplane/adapter/in/web/WorkflowResponse.java`

The `WorkflowDefinition` domain object is converted to a `WorkflowResponse` HTTP DTO. This strips internal fields the client shouldn't see and formats the response according to the API contract.

---

## 9. HTTP Response

```
HTTP/1.1 201 Created
Location: /api/v1/workflows/{uuid}
ETag: "0"
Content-Type: application/json

{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Order processing",
  "description": "Validate, charge, and notify",
  "definitionVersion": 1,
  "versionStatus": "DRAFT",
  "lifecycleStatus": "ACTIVE",
  "lockVersion": 0,
  "tasks": [...],
  "dependencies": [...],
  "createdAt": "2026-09-15T12:00:00Z",
  "updatedAt": "2026-09-15T12:00:00Z"
}
```

The `ETag: "0"` is the `lock_version` — the client must send it back as `If-Match: "0"` on the next mutation (update, publish, or archive).

---

## 10. Summary: Complete Call Chain

```
POST /api/v1/workflows
  │
  ├─ TenantContextFilter.doFilter()
  │    └─ Extract tenantId from JWT → store in TenantContext
  │
  ├─ WorkflowController.create()
  │    ├─ @Valid → Bean Validation on WorkflowRequest (400 if fails)
  │    ├─ TenantContextFilter.requireTenant() → TenantId
  │    ├─ toDraft(request) → WorkflowDraft
  │    │    └─ new WorkflowDraft() constructor
  │    │         └─ validate() → DAG validation (Kahn's algorithm)
  │    │              └─ throws DomainValidationException if invalid (422)
  │    └─ service.create(tenantId, draft)
  │         └─ WorkflowService.create()
  │              ├─ Objects.requireNonNull(tenantId)
  │              └─ repository.create(tenantId, draft)
  │                   └─ JdbcWorkflowRepository.create() [@Transactional]
  │                        ├─ INSERT workflow_definition (ACTIVE, tenant_id)
  │                        ├─ INSERT workflow_version (v1, DRAFT)
  │                        ├─ replaceGraph()
  │                        │    ├─ INSERT workflow_task (× N tasks)
  │                        │    └─ INSERT workflow_dependency (× M edges)
  │                        └─ findById() → read back full workflow
  │                             ├─ SELECT_CURRENT (LATERAL join)
  │                             ├─ loadTasks()
  │                             └─ loadDependencies()
  │
  ├─ WorkflowResponse.from(definition) → HTTP DTO
  │
  └─ Return 201 Created + Location + ETag: "0"
```

---

## 11. Why Each Layer Exists (Clean Architecture)

| Layer | Knows About | Doesn't Know About | Why |
|---|---|---|---|
| **Domain** (`WorkflowDraft`) | Pure Java, DAG validation | Spring, HTTP, JDBC, JSON | Invariants are eternal — they don't change when you swap databases or frameworks |
| **Application** (`WorkflowService`) | Domain + repository port | Spring, HTTP, JDBC | Use cases are framework-free and testable with a mock repository |
| **Control-Plane** (`WorkflowController`, `JdbcWorkflowRepository`) | Everything | — | Frameworks change; domain logic doesn't. Isolating framework code makes migration safe |

The **ArchUnit test** enforces this at build time — if anyone imports Spring in the domain module, the build fails.

---

*See also: [documentation.md](../documentation.md) for the full project walkthrough, [ADR-001](../docs/adr/001-phase-1-architecture.md) for the major design decisions.*
