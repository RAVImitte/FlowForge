# Update Workflow — Complete Code Flow

> `PUT /api/v1/workflows/{id}` — Updates the draft version (or creates a new draft if the current version is published). Requires `If-Match` header.

---

## Step 1: HTTP Request

```
PUT /api/v1/workflows/550e8400-e29b-41d4-a716-446655440000
If-Match: "0"
Content-Type: application/json
Authorization: Bearer <JWT>

{
  "name": "Order processing v2",
  "tasks": [
    {"key": "VALIDATE_ORDER", "name": "Validate order", "type": "NOOP", "configuration": {}}
  ]
}
```

---

## Step 2: TenantContextFilter — Extract Tenant Identity

Same as all endpoints.

---

## Step 3: WorkflowController.update()

**File:** `flowforge-control-plane/.../adapter/in/web/WorkflowController.java` (line 70–86)

```java
@PutMapping("/{id}")
ResponseEntity<WorkflowResponse> update(
        HttpServletRequest httpRequest,
        @PathVariable UUID id,
        @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch,
        @Valid @RequestBody WorkflowRequest request
) {
    WorkflowDefinition updated = service.update(
            TenantContextFilter.requireTenant(httpRequest),
            id,
            parseEtag(ifMatch),
            toDraft(request)
    );
    return ResponseEntity.ok()
            .eTag(etag(updated.lockVersion()))
            .body(WorkflowResponse.from(updated));
}
```

**What happens:**
1. `@Valid @RequestBody WorkflowRequest request` — Bean Validation runs.
2. `parseEtag(ifMatch)` — Parses the `If-Match` header:
   - If missing → throws `PreconditionRequiredException` → `428 Precondition Required`
   - If malformed (not `"[0-9]+"`) → throws `IllegalArgumentException` → `400 Bad Request`
   - Extracts the numeric version from `"0"` → `0L`
3. `toDraft(request)` — Converts HTTP DTO → `WorkflowDraft` (runs DAG validation)
4. `service.update(tenantId, id, expectedLockVersion, draft)` — Delegates to service.

---

## Step 4: WorkflowService.update()

**File:** `flowforge-application/.../workflow/WorkflowService.java` (line 32–39)

```java
public WorkflowDefinition update(
        TenantId tenantId, UUID id, long expectedLockVersion, WorkflowDraft draft
) {
    return repository.update(Objects.requireNonNull(tenantId), id, expectedLockVersion, draft);
}
```

Validates `tenantId` is not null, delegates to repository.

---

## Step 5: JdbcWorkflowRepository.update()

**File:** `flowforge-control-plane/.../adapter/out/persistence/JdbcWorkflowRepository.java` (line 170–226)

```java
@Override
@Transactional
public WorkflowDefinition update(
        TenantId tenantId, UUID id, long expectedLockVersion, WorkflowDraft draft
) {
    // 5a. Lock and verify version
    lockAndCheck(tenantId, id, expectedLockVersion);

    // 5b. Check if a DRAFT version exists
    Optional<UUID> existingDraft = jdbc.sql("""
            SELECT id FROM workflow_version
             WHERE workflow_id = :workflowId AND version_status = 'DRAFT'
            """).param("workflowId", id).query(UUID.class).optional();

    UUID versionId;
    if (existingDraft.isPresent()) {
        // 5c-i. Update existing DRAFT
        versionId = existingDraft.get();
        jdbc.sql("""
                UPDATE workflow_version
                   SET name = :name, description = :description,
                       max_concurrent_executions = :maxConcurrentExecutions,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = :id
                """)...update();
    } else {
        // 5c-ii. Create new DRAFT version
        int nextVersion = jdbc.sql("""
                SELECT COALESCE(MAX(version_number), 0) + 1
                  FROM workflow_version WHERE workflow_id = :workflowId
                """).param("workflowId", id).query(Integer.class).single();
        versionId = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO workflow_version(
                    id, workflow_id, version_number, version_status, name, description,
                    max_concurrent_executions
                ) VALUES (
                    :id, :workflowId, :versionNumber, 'DRAFT', :name, :description,
                    :maxConcurrentExecutions
                )
                """)...update();
    }

    // 5d. Replace task graph
    replaceGraph(versionId, draft);

    // 5e. Increment lock_version
    incrementVersion(tenantId, id);

    // 5f. Read back
    return findById(tenantId, id).orElseThrow();
}
```

### 5a. `lockAndCheck()` — Pessimistic Lock + Version Check

**File:** `JdbcWorkflowRepository.java` (line 264–288)

```java
private void lockAndCheck(TenantId tenantId, UUID id, long expectedLockVersion) {
    Optional<Map<String, Object>> row = jdbc.sql("""
            SELECT lock_version, lifecycle_status
              FROM workflow_definition
             WHERE id = :id
               AND tenant_id = :tenantId
             FOR UPDATE
            """)
            .param("id", id)
            .param("tenantId", tenantId.value())
            .query((rs, rowNum) -> Map.<String, Object>of(
                    "lock_version", rs.getLong("lock_version"),
                    "lifecycle_status", rs.getString("lifecycle_status")
            ))
            .optional();
    if (row.isEmpty() || !"ACTIVE".equals(row.get().get("lifecycle_status"))) {
        throw new WorkflowNotFoundException(id);
    }
    long actual = ((Number) row.get().get("lock_version")).longValue();
    if (actual != expectedLockVersion) {
        throw new WorkflowConflictException(
                "Workflow was modified concurrently; expected version "
                        + expectedLockVersion + " but found " + actual);
    }
}
```

**What happens:**
1. `SELECT ... FOR UPDATE` — acquires a **pessimistic row lock** on the `workflow_definition` row. Concurrent transactions block here until this transaction commits.
2. Checks the row exists, is `ACTIVE`, and belongs to the tenant.
3. Compares `lock_version` with the `If-Match` value. If mismatch → `WorkflowConflictException` → `409 Conflict`.

**Why both pessimistic lock AND optimistic version check?** The `FOR UPDATE` serializes concurrent transactions. The version check catches stale clients who read the workflow, waited, then tried to update with an outdated ETag. Belt-and-suspenders for correctness.

### 5b–5c. Draft Decision Logic

- **If a DRAFT exists** → update its name/description and replace the task graph. The published version is untouched.
- **If no DRAFT exists** (the current version was published) → create a **new version** with `version_number = MAX + 1` in `DRAFT` status. The published version remains immutable.

### 5d. `replaceGraph()` — Delete + Re-insert Tasks/Dependencies

Same as in `create()` — deletes all tasks and dependencies for the version, then re-inserts them. Simple "replace" strategy within the transaction.

### 5e. `incrementVersion()` — Bump `lock_version`

```java
UPDATE workflow_definition
   SET lock_version = lock_version + 1, updated_at = CURRENT_TIMESTAMP
 WHERE id = :id AND tenant_id = :tenantId
```

This invalidates the old ETag. The next mutation must use the new ETag.

---

## Step 6: HTTP Response

```
HTTP/1.1 200 OK
ETag: "1"
Content-Type: application/json

{
  "id": "550e8400-...",
  "name": "Order processing v2",
  "definitionVersion": 2,
  "versionStatus": "DRAFT",
  "lockVersion": 1,
  ...
}
```

The `ETag` is now `"1"` (incremented from `"0"`).

---

## Call Chain Summary

```
PUT /api/v1/workflows/{id}  (If-Match: "0")
  │
  ├─ TenantContextFilter → extract tenantId
  │
  ├─ WorkflowController.update()
  │    ├─ @Valid → Bean Validation (400 if fails)
  │    ├─ parseEtag(ifMatch) → 428 if missing, 400 if malformed
  │    ├─ toDraft(request) → WorkflowDraft (DAG validation → 422 if invalid)
  │    └─ service.update(tenantId, id, expectedLockVersion, draft)
  │         └─ WorkflowService.update()
  │              └─ repository.update(tenantId, id, expectedLockVersion, draft)
  │                   └─ JdbcWorkflowRepository.update() [@Transactional]
  │                        ├─ lockAndCheck()
  │                        │    ├─ SELECT ... FOR UPDATE (pessimistic lock)
  │                        │    ├─ Check ACTIVE + tenant_id → 404 if not found
  │                        │    └─ Check lock_version == expected → 409 if mismatch
  │                        ├─ Check if DRAFT exists
  │                        │    ├─ YES → UPDATE workflow_version (name, description)
  │                        │    └─ NO → INSERT new workflow_version (vN+1, DRAFT)
  │                        ├─ replaceGraph() → delete + re-insert tasks/dependencies
  │                        ├─ incrementVersion() → lock_version + 1
  │                        └─ findById() → read back
  │
  ├─ WorkflowResponse.from(definition) → HTTP DTO
  │
  └─ Return 200 OK + ETag: "1"
```
