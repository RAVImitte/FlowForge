# List Workflows — Complete Code Flow

> `GET /api/v1/workflows?page=0&size=20` — Lists active workflows for the tenant with pagination.

---

## Step 1: HTTP Request

```
GET /api/v1/workflows?page=0&size=20
Authorization: Bearer <JWT>
```

Query parameters:
- `page` (default `0`) — zero-based page index
- `size` (default `20`) — page size, must be 1–100

---

## Step 2: TenantContextFilter — Extract Tenant Identity

Same as all endpoints — extracts tenant ID from JWT.

---

## Step 3: WorkflowController.list()

**File:** `flowforge-control-plane/.../adapter/in/web/WorkflowController.java` (line 59–68)

```java
@GetMapping
WorkflowPageResponse list(
        HttpServletRequest request,
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "20") int size
) {
    return WorkflowPageResponse.from(
            service.list(TenantContextFilter.requireTenant(request), page, size)
    );
}
```

**What happens:**
1. Spring binds `page` and `size` query params with defaults.
2. Delegates to `service.list(tenantId, page, size)`.
3. Wraps the `PageResult<WorkflowDefinition>` in a `WorkflowPageResponse` DTO.

---

## Step 4: WorkflowService.list()

**File:** `flowforge-application/.../workflow/WorkflowService.java` (line 26–30)

```java
public PageResult<WorkflowDefinition> list(TenantId tenantId, int page, int size) {
    if (page < 0) throw new IllegalArgumentException("page must be at least 0");
    if (size < 1 || size > 100) throw new IllegalArgumentException("size must be between 1 and 100");
    return repository.findAll(Objects.requireNonNull(tenantId), page, size);
}
```

**What happens:**
1. Validates `page ≥ 0` — negative pages are rejected with `400 Bad Request`.
2. Validates `1 ≤ size ≤ 100` — prevents excessively large pages.
3. Delegates to `repository.findAll(tenantId, page, size)`.

**Why needed:** The service is the validation boundary for pagination parameters. The repository trusts the service to provide valid inputs.

---

## Step 5: JdbcWorkflowRepository.findAll()

**File:** `flowforge-control-plane/.../adapter/out/persistence/JdbcWorkflowRepository.java` (line 119–148)

```java
@Override
@Transactional(readOnly = true)
public PageResult<WorkflowDefinition> findAll(TenantId tenantId, int page, int size) {
    // 5a. Count total active workflows for this tenant
    long total = jdbc.sql("""
            SELECT COUNT(*)
              FROM workflow_definition
             WHERE tenant_id = :tenantId
               AND lifecycle_status = 'ACTIVE'
            """)
            .param("tenantId", tenantId.value())
            .query(Long.class)
            .single();

    // 5b. Fetch a page of workflow IDs
    List<UUID> ids = jdbc.sql("""
            SELECT id
              FROM workflow_definition
             WHERE tenant_id = :tenantId
               AND lifecycle_status = 'ACTIVE'
             ORDER BY created_at DESC, id
             LIMIT :limit OFFSET :offset
            """)
            .param("tenantId", tenantId.value())
            .param("limit", size)
            .param("offset", page * size)
            .query(UUID.class)
            .list();

    // 5c. Load each workflow by ID (N+1 pattern)
    List<WorkflowDefinition> workflows = ids.stream()
            .map(id -> findById(tenantId, id).orElseThrow())
            .toList();
    return new PageResult<>(workflows, page, size, total);
}
```

**What happens:**
1. **Count query** — `SELECT COUNT(*)` for total active workflows belonging to this tenant. Used to compute `totalPages`.
2. **ID query** — Fetches a page of UUIDs ordered by `created_at DESC, id` with `LIMIT/OFFSET`.
3. **N+1 load** — Calls `findById(tenantId, id)` for each UUID. Each call runs the `SELECT_CURRENT` LATERAL join + `loadTasks()` + `loadDependencies()`.

**Why the N+1 pattern?** For Phase 1's scale (tens to hundreds of workflows), the N+1 is acceptable and keeps the code simple. Each `findById` reuses the same tested code path. For larger scales, a batch query with `IN (:ids)` would be more efficient.

**Why `@Transactional(readOnly = true)`:** Read-only transaction — no locks, PostgreSQL can optimize.

---

## Step 6: WorkflowPageResponse.from()

**File:** `flowforge-control-plane/.../adapter/in/web/WorkflowPageResponse.java`

Wraps `PageResult<WorkflowDefinition>` into:
```json
{
  "items": [...],
  "page": 0,
  "size": 20,
  "totalElements": 42,
  "totalPages": 3
}
```

`totalPages = ceil(totalElements / size)`.

---

## Step 7: HTTP Response

```
HTTP/1.1 200 OK
Content-Type: application/json

{
  "items": [
    { "id": "...", "name": "Order processing", "versionStatus": "DRAFT", ... },
    { "id": "...", "name": "Payment flow", "versionStatus": "PUBLISHED", ... }
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
GET /api/v1/workflows?page=0&size=20
  │
  ├─ TenantContextFilter.doFilter() → extract tenantId
  │
  ├─ WorkflowController.list()
  │    └─ service.list(tenantId, page, size)
  │         └─ WorkflowService.list()
  │              ├─ Validate page ≥ 0, 1 ≤ size ≤ 100
  │              └─ repository.findAll(tenantId, page, size)
  │                   └─ JdbcWorkflowRepository.findAll() [@Transactional(readOnly)]
  │                        ├─ SELECT COUNT(*) → total
  │                        ├─ SELECT id ... LIMIT/OFFSET → page of IDs
  │                        └─ findById() × N (N+1 pattern)
  │                             ├─ SELECT_CURRENT (LATERAL join)
  │                             ├─ loadTasks()
  │                             └─ loadDependencies()
  │
  ├─ WorkflowPageResponse.from(pageResult) → HTTP DTO
  │
  └─ Return 200 OK
```
