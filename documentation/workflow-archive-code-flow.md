# Archive Workflow — Complete Code Flow

> `DELETE /api/v1/workflows/{id}` — Soft-deletes (archives) a workflow. Requires `If-Match` header.

---

## Step 1: HTTP Request

```
DELETE /api/v1/workflows/550e8400-e29b-41d4-a716-446655440000
If-Match: "1"
Authorization: Bearer <JWT>
```

---

## Step 2: TenantContextFilter — Extract Tenant Identity

Same as all endpoints.

---

## Step 3: WorkflowController.archive()

**File:** `flowforge-control-plane/.../adapter/in/web/WorkflowController.java` (line 102–110)

```java
@DeleteMapping("/{id}")
ResponseEntity<Void> archive(
        HttpServletRequest request,
        @PathVariable UUID id,
        @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch
) {
    service.archive(TenantContextFilter.requireTenant(request), id, parseEtag(ifMatch));
    return ResponseEntity.noContent().build();
}
```

**What happens:**
1. `parseEtag(ifMatch)` — `428` if missing, `400` if malformed.
2. `service.archive(tenantId, id, expectedLockVersion)` — Delegates to service.
3. Returns `204 No Content` (empty body).

---

## Step 4: WorkflowService.archive()

**File:** `flowforge-application/.../workflow/WorkflowService.java` (line 45–47)

```java
public void archive(TenantId tenantId, UUID id, long expectedLockVersion) {
    repository.archive(Objects.requireNonNull(tenantId), id, expectedLockVersion);
}
```

Validates `tenantId`, delegates to repository. Returns `void` — no read-back needed.

---

## Step 5: JdbcWorkflowRepository.archive()

**File:** `flowforge-control-plane/.../adapter/out/persistence/JdbcWorkflowRepository.java` (line 247–262)

```java
@Override
@Transactional
public void archive(TenantId tenantId, UUID id, long expectedLockVersion) {
    // 5a. Lock and verify version
    lockAndCheck(tenantId, id, expectedLockVersion);

    // 5b. Soft-delete: set lifecycle_status = 'ARCHIVED'
    jdbc.sql("""
            UPDATE workflow_definition
               SET lifecycle_status = 'ARCHIVED',
                   lock_version = lock_version + 1,
                   updated_at = CURRENT_TIMESTAMP
             WHERE id = :id
               AND tenant_id = :tenantId
            """)
            .param("id", id)
            .param("tenantId", tenantId.value())
            .update();
}
```

### 5a. `lockAndCheck()` — Pessimistic Lock + Version Check

Same as `update()` and `publish()` — `SELECT ... FOR UPDATE`, verify `ACTIVE` + tenant + `lock_version`.

### 5b. Soft-Delete

```sql
UPDATE workflow_definition
   SET lifecycle_status = 'ARCHIVED',
       lock_version = lock_version + 1,
       updated_at = CURRENT_TIMESTAMP
 WHERE id = :id
   AND tenant_id = :tenantId
```

**What happens:**
- Sets `lifecycle_status` from `ACTIVE` to `ARCHIVED`
- Increments `lock_version` (though this is the last mutation possible)
- Sets `updated_at` to current timestamp

**Why soft-delete (not hard delete)?**
- **Audit trail** — archived workflows remain in the database for compliance and audit
- **Referential integrity** — existing executions reference workflow versions; hard-deleting would break foreign keys
- **Recoverability** — if needed, an archived workflow could be un-archived (though no API endpoint exists for this yet)
- **Safety** — no data is lost; the workflow simply becomes invisible to `SELECT_CURRENT` (which filters `lifecycle_status = 'ACTIVE'`)

**What happens to in-flight executions?** Archiving does not cancel running executions. The `workflow_definition` row is marked `ARCHIVED`, but the `workflow_version` rows (including tasks and dependencies) remain intact. In-flight executions reference the version, not the definition, so they continue to completion. New executions cannot be started because `start()` checks for a PUBLISHED version on an ACTIVE workflow.

---

## Step 6: HTTP Response

```
HTTP/1.1 204 No Content
```

Empty body. The workflow is now archived — subsequent `GET /api/v1/workflows/{id}` returns `404 Not Found`.

---

## Call Chain Summary

```
DELETE /api/v1/workflows/{id}  (If-Match: "1")
  │
  ├─ TenantContextFilter → extract tenantId
  │
  ├─ WorkflowController.archive()
  │    ├─ parseEtag(ifMatch) → 428 if missing, 400 if malformed
  │    └─ service.archive(tenantId, id, expectedLockVersion)
  │         └─ WorkflowService.archive()
  │              └─ repository.archive(tenantId, id, expectedLockVersion)
  │                   └─ JdbcWorkflowRepository.archive() [@Transactional]
  │                        ├─ lockAndCheck()
  │                        │    ├─ SELECT ... FOR UPDATE
  │                        │    ├─ Check ACTIVE + tenant → 404 if not found
  │                        │    └─ Check lock_version → 409 if mismatch
  │                        └─ UPDATE workflow_definition
  │                             SET lifecycle_status = 'ARCHIVED',
  │                                 lock_version = lock_version + 1
  │
  └─ Return 204 No Content
```
