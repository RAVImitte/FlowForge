# List Executions — Complete Code Flow

> `GET /api/v1/executions?page=0&size=20&status=RUNNING` — Lists executions for the tenant with pagination and optional status filter.

---

## Step 1: HTTP Request

```
GET /api/v1/executions?page=0&size=20&status=RUNNING
Authorization: Bearer <JWT>
```

Query parameters:
- `page` (default `0`) — zero-based page index
- `size` (default `20`) — page size, must be 1–100
- `status` (optional) — filter by `WorkflowRunStatus` enum: `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`, `TIMED_OUT`. Case-insensitive. `ALL` (or omitted) = no filter.

---

## Step 2: TenantContextFilter — Extract Tenant Identity

Same as all endpoints.

---

## Step 3: WorkflowExecutionController.list()

**File:** `flowforge-control-plane/.../adapter/in/web/WorkflowExecutionController.java` (line 50–64)

```java
@GetMapping("/executions")
ExecutionPageResponse list(
        HttpServletRequest request,
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "20") int size,
        @RequestParam(required = false) String status
) {
    WorkflowRunStatus statusFilter = null;
    if (status != null && !status.isBlank() && !"ALL".equalsIgnoreCase(status)) {
        statusFilter = WorkflowRunStatus.valueOf(status.toUpperCase());
    }
    return ExecutionPageResponse.from(
            service.list(TenantContextFilter.requireTenant(request), page, size, statusFilter)
    );
}
```

**What happens:**
1. Spring binds `page`, `size`, and `status` query params.
2. If `status` is provided and not `ALL` (case-insensitive), it's parsed into a `WorkflowRunStatus` enum value. If the string doesn't match any enum constant, `valueOf()` throws `IllegalArgumentException` → `400 Bad Request`.
3. Delegates to `service.list(tenantId, page, size, statusFilter)`.
4. Wraps the `PageResult<ExecutionSummary>` in an `ExecutionPageResponse` DTO.

---

## Step 4: WorkflowExecutionService.list()

**File:** `flowforge-application/.../execution/WorkflowExecutionService.java` (line 133–138)

```java
public PageResult<ExecutionSummary> list(TenantId tenantId, int page, int size, WorkflowRunStatus statusFilter) {
    Objects.requireNonNull(tenantId, "tenantId must not be null");
    if (page < 0) throw new IllegalArgumentException("page must be at least 0");
    if (size < 1 || size > 100) throw new IllegalArgumentException("size must be between 1 and 100");
    return repository.list(tenantId, page, size, statusFilter);
}
```

**What happens:**
1. Validates `tenantId` is not null.
2. Validates `page ≥ 0` and `1 ≤ size ≤ 100`.
3. Delegates to `repository.list(tenantId, page, size, statusFilter)`.

**Why `statusFilter` can be null:** A `null` status filter means "all statuses" — no `WHERE status = ?` clause is added to the query. This is different from filtering by a specific status.

---

## Step 5: ExecutionRepository.list() → JdbcExecutionRepository

**File:** `flowforge-control-plane/.../adapter/out/persistence/JdbcExecutionRepository.java`

The `list()` method returns `PageResult<ExecutionSummary>`, not `PageResult<WorkflowExecution>`. `ExecutionSummary` is a lightweight projection — it doesn't include tasks, attempts, or events. This keeps the list endpoint fast even for workflows with hundreds of tasks.

### ExecutionSummary Fields

**File:** `flowforge-application/.../execution/ExecutionSummary.java`

```java
public record ExecutionSummary(
        UUID id,
        String tenantId,
        UUID workflowId,
        int workflowVersion,
        WorkflowRunStatus status,
        long stateVersion,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt,
        int totalTasks,
        int completedTasks
) {}
```

The `totalTasks` and `completedTasks` fields give a quick progress indicator without loading the full task graph.

### SQL Query (conceptual)

```sql
-- Count total
SELECT COUNT(*)
  FROM workflow_execution e
  JOIN workflow_definition w ON e.workflow_id = w.id
 WHERE e.tenant_id = :tenantId
   [AND e.status = :status]   -- only if statusFilter != null

-- Fetch page
SELECT e.id, e.tenant_id, e.workflow_id, e.workflow_version, e.status,
       e.state_version, e.created_at, e.started_at, e.finished_at,
       COUNT(t.id) AS total_tasks,
       COUNT(t.id) FILTER (WHERE t.status IN ('SUCCEEDED', 'FAILED', 'SKIPPED')) AS completed_tasks
  FROM workflow_execution e
  LEFT JOIN task_run t ON t.workflow_execution_id = e.id
 WHERE e.tenant_id = :tenantId
   [AND e.status = :status]
  GROUP BY e.id
  ORDER BY e.created_at DESC, e.id
  LIMIT :limit OFFSET :offset
```

**Why `@Transactional(readOnly = true)`:** Read-only — no locks, PostgreSQL can optimize.

---

## Step 6: ExecutionPageResponse.from()

**File:** `flowforge-control-plane/.../adapter/in/web/ExecutionPageResponse.java`

Wraps `PageResult<ExecutionSummary>` into:
```json
{
  "items": [...],
  "page": 0,
  "size": 20,
  "totalElements": 42,
  "totalPages": 3
}
```

---

## Step 7: HTTP Response

```
HTTP/1.1 200 OK
Content-Type: application/json

{
  "items": [
    {
      "id": "660e8400-...",
      "workflowId": "550e8400-...",
      "workflowVersion": 1,
      "status": "RUNNING",
      "stateVersion": 5,
      "createdAt": "2026-09-15T12:00:00Z",
      "startedAt": "2026-09-15T12:00:01Z",
      "finishedAt": null,
      "totalTasks": 12,
      "completedTasks": 3
    }
  ],
  "page": 0,
  "size": 20,
  "totalElements": 42,
  "totalPages": 3
}
```

---

## Call Chain Summary

```
GET /api/v1/executions?page=0&size=20&status=RUNNING
  │
  ├─ TenantContextFilter → extract tenantId
  │
  ├─ WorkflowExecutionController.list()
  │    ├─ Parse status → WorkflowRunStatus enum (400 if invalid)
  │    └─ service.list(tenantId, page, size, statusFilter)
  │         └─ WorkflowExecutionService.list()
  │              ├─ Validate page ≥ 0, 1 ≤ size ≤ 100
  │              └─ repository.list(tenantId, page, size, statusFilter)
  │                   └─ JdbcExecutionRepository.list() [@Transactional(readOnly)]
  │                        ├─ SELECT COUNT(*) → total
  │                        └─ SELECT ... LIMIT/OFFSET → page of ExecutionSummary
  │
  ├─ ExecutionPageResponse.from(pageResult) → HTTP DTO
  │
  └─ Return 200 OK
```
