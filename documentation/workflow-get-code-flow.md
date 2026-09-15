# Get Workflow by ID — Complete Code Flow

> `GET /api/v1/workflows/{id}` — Retrieves a single workflow definition by ID.

---

## Step 1: HTTP Request

```
GET /api/v1/workflows/550e8400-e29b-41d4-a716-446655440000
Authorization: Bearer <JWT>
```

No `If-Match` header required — this is a read operation.

---

## Step 2: TenantContextFilter — Extract Tenant Identity

**File:** `flowforge-control-plane/.../config/TenantContextFilter.java`

The servlet filter extracts the tenant ID from the JWT and stores it in `TenantContext`. Same as all endpoints — ensures tenant isolation.

---

## Step 3: WorkflowController.get()

**File:** `flowforge-control-plane/.../adapter/in/web/WorkflowController.java` (line 51–57)

```java
@GetMapping("/{id}")
ResponseEntity<WorkflowResponse> get(HttpServletRequest request, @PathVariable UUID id) {
    WorkflowDefinition workflow = service.get(TenantContextFilter.requireTenant(request), id);
    return ResponseEntity.ok()
            .eTag(etag(workflow.lockVersion()))
            .body(WorkflowResponse.from(workflow));
}
```

**What happens:**
1. `@PathVariable UUID id` — Spring extracts the UUID from the URL path.
2. `TenantContextFilter.requireTenant(request)` — Gets the tenant ID from the request context.
3. `service.get(tenantId, id)` — Delegates to the application service.
4. Builds `200 OK` response with `ETag` header (the current `lock_version`).

**Why the ETag on a GET?** The client needs the ETag to perform subsequent mutations (update, publish, archive). Including it on every GET means the client always has the latest version without a separate call.

---

## Step 4: WorkflowService.get()

**File:** `flowforge-application/.../workflow/WorkflowService.java` (line 21–24)

```java
public WorkflowDefinition get(TenantId tenantId, UUID id) {
    return repository.findById(Objects.requireNonNull(tenantId), id)
            .orElseThrow(() -> new WorkflowNotFoundException(id));
}
```

**What happens:**
1. Validates `tenantId` is not null.
2. Calls `repository.findById(tenantId, id)`.
3. If the workflow doesn't exist (or belongs to a different tenant, or is archived), `Optional` is empty → throws `WorkflowNotFoundException`.

**Why needed:** The service is the use-case boundary. It translates the repository's `Optional<WorkflowDefinition>` into a domain-specific exception (`WorkflowNotFoundException`) that the `ApiExceptionHandler` maps to `404 Not Found`.

---

## Step 5: JdbcWorkflowRepository.findById()

**File:** `flowforge-control-plane/.../adapter/out/persistence/JdbcWorkflowRepository.java` (line 91–117)

```java
@Override
@Transactional(readOnly = true)
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
                        loadTasks(versionId),
                        loadDependencies(versionId),
                        instant(rs.getObject("created_at")),
                        instant(rs.getObject("updated_at")),
                        instant(rs.getObject("published_at"))
                );
            })
            .optional();
}
```

**The SELECT_CURRENT query:**

```sql
SELECT w.id, w.tenant_id, w.lock_version, w.lifecycle_status, w.created_at, w.updated_at,
       v.id AS workflow_version_id, v.version_number, v.version_status,
       v.name, v.description, v.max_concurrent_executions, v.published_at
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

**What happens:**
1. The `LATERAL` join picks the "current" version: DRAFT if one exists (priority 0), otherwise the latest PUBLISHED (priority 1, descending version number).
2. The `WHERE` clause filters by `id`, `tenant_id`, and `lifecycle_status = 'ACTIVE'` — archived workflows are invisible.
3. `loadTasks(versionId)` — separate query to fetch all tasks ordered by `position`.
4. `loadDependencies(versionId)` — separate query to fetch all dependency edges.

**Why `@Transactional(readOnly = true)`:** Marks the transaction as read-only, allowing PostgreSQL to optimize the query. No locks are acquired.

**Why tenant_id in the WHERE clause:** Even though the tenant ID was extracted from the JWT, the database enforces isolation independently. A workflow belonging to tenant A is simply not found by tenant B — returning 404 rather than 403 to avoid information disclosure.

---

## Step 6: WorkflowResponse.from() — Domain → HTTP DTO

**File:** `flowforge-control-plane/.../adapter/in/web/WorkflowResponse.java`

Converts the `WorkflowDefinition` domain object to a `WorkflowResponse` HTTP DTO.

---

## Step 7: HTTP Response

```
HTTP/1.1 200 OK
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

---

## Call Chain Summary

```
GET /api/v1/workflows/{id}
  │
  ├─ TenantContextFilter.doFilter() → extract tenantId
  │
  ├─ WorkflowController.get()
  │    ├─ TenantContextFilter.requireTenant() → TenantId
  │    └─ service.get(tenantId, id)
  │         └─ WorkflowService.get()
  │              ├─ Objects.requireNonNull(tenantId)
  │              └─ repository.findById(tenantId, id)
  │                   └─ JdbcWorkflowRepository.findById() [@Transactional(readOnly)]
  │                        ├─ SELECT_CURRENT (LATERAL join)
  │                        ├─ loadTasks()
  │                        └─ loadDependencies()
  │              └─ orElseThrow → WorkflowNotFoundException (404)
  │
  ├─ WorkflowResponse.from(definition) → HTTP DTO
  │
  └─ Return 200 OK + ETag
```
