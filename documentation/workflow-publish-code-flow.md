# Publish Workflow — Complete Code Flow

> `POST /api/v1/workflows/{id}/publish` — Promotes the current DRAFT version to PUBLISHED. Requires `If-Match` header.

---

## Step 1: HTTP Request

```
POST /api/v1/workflows/550e8400-e29b-41d4-a716-446655440000/publish
If-Match: "0"
Authorization: Bearer <JWT>
```

No request body needed — publishing is a state transition, not a content update.

---

## Step 2: TenantContextFilter — Extract Tenant Identity

Same as all endpoints.

---

## Step 3: WorkflowController.publish()

**File:** `flowforge-control-plane/.../adapter/in/web/WorkflowController.java` (line 88–100)

```java
@PostMapping("/{id}/publish")
ResponseEntity<WorkflowResponse> publish(
        HttpServletRequest request,
        @PathVariable UUID id,
        @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch
) {
    WorkflowDefinition published = service.publish(
            TenantContextFilter.requireTenant(request), id, parseEtag(ifMatch)
    );
    return ResponseEntity.ok()
            .eTag(etag(published.lockVersion()))
            .body(WorkflowResponse.from(published));
}
```

**What happens:**
1. `parseEtag(ifMatch)` — Parses `If-Match` header → `428` if missing, `400` if malformed.
2. `service.publish(tenantId, id, expectedLockVersion)` — Delegates to service.
3. Returns `200 OK` with new ETag.

---

## Step 4: WorkflowService.publish()

**File:** `flowforge-application/.../workflow/WorkflowService.java` (line 41–43)

```java
public WorkflowDefinition publish(TenantId tenantId, UUID id, long expectedLockVersion) {
    return repository.publish(Objects.requireNonNull(tenantId), id, expectedLockVersion);
}
```

Validates `tenantId`, delegates to repository.

---

## Step 5: JdbcWorkflowRepository.publish()

**File:** `flowforge-control-plane/.../adapter/out/persistence/JdbcWorkflowRepository.java` (line 228–245)

```java
@Override
@Transactional
public WorkflowDefinition publish(TenantId tenantId, UUID id, long expectedLockVersion) {
    // 5a. Lock and verify version
    lockAndCheck(tenantId, id, expectedLockVersion);

    // 5b. Flip DRAFT → PUBLISHED
    int changed = jdbc.sql("""
            UPDATE workflow_version
               SET version_status = 'PUBLISHED',
                   published_at = CURRENT_TIMESTAMP,
                   updated_at = CURRENT_TIMESTAMP
             WHERE workflow_id = :workflowId
               AND version_status = 'DRAFT'
            """).param("workflowId", id).update();

    // 5c. No draft to publish → conflict
    if (changed == 0) {
        throw new WorkflowConflictException("Workflow has no draft version to publish");
    }

    // 5d. Increment lock_version
    incrementVersion(tenantId, id);

    // 5e. Read back
    return findById(tenantId, id).orElseThrow();
}
```

### 5a. `lockAndCheck()` — Pessimistic Lock + Version Check

Same as in `update()` — `SELECT ... FOR UPDATE` on `workflow_definition`, verify `ACTIVE` + tenant + `lock_version` match.

### 5b. Flip DRAFT → PUBLISHED

```sql
UPDATE workflow_version
   SET version_status = 'PUBLISHED',
       published_at = CURRENT_TIMESTAMP,
       updated_at = CURRENT_TIMESTAMP
 WHERE workflow_id = :workflowId
   AND version_status = 'DRAFT'
```

- Sets `version_status` from `DRAFT` to `PUBLISHED`
- Sets `published_at` to the current timestamp (was NULL for DRAFT)
- The `CHECK` constraint on `workflow_version` ensures `published_at` is non-NULL for PUBLISHED versions

### 5c. No Draft → Conflict

If `changed == 0`, there was no DRAFT version to publish. This happens when:
- The workflow was created and published, but no new draft was created via `PUT`
- Someone tries to publish an already-published workflow

Throws `WorkflowConflictException` → `409 Conflict` with message "Workflow has no draft version to publish".

### 5d. `incrementVersion()` — Bump `lock_version`

Same as `update()` — `lock_version + 1` on `workflow_definition`.

### 5e. Read Back

`findById()` returns the now-PUBLISHED version with `publishedAt` set.

---

## Key Invariant: Published Versions Are Immutable

Once a version is PUBLISHED:
- Its `name`, `description`, `tasks`, and `dependencies` are **frozen** — never mutated
- The `published_at` timestamp is set permanently
- To change the workflow, you call `PUT` (update), which creates a **new DRAFT** with the next version number
- The PUBLISHED version remains in the database for execution and audit

This is enforced by:
1. The `update()` method only modifies DRAFT versions (or creates new ones)
2. The `publish()` method only touches `version_status` and `published_at` — never the task graph
3. The partial unique index `uq_workflow_single_draft` ensures at most one DRAFT per workflow

---

## Step 6: HTTP Response

```
HTTP/1.1 200 OK
ETag: "1"
Content-Type: application/json

{
  "id": "550e8400-...",
  "name": "Order processing",
  "definitionVersion": 1,
  "versionStatus": "PUBLISHED",
  "lifecycleStatus": "ACTIVE",
  "lockVersion": 1,
  "publishedAt": "2026-09-15T12:05:00Z",
  ...
}
```

---

## Call Chain Summary

```
POST /api/v1/workflows/{id}/publish  (If-Match: "0")
  │
  ├─ TenantContextFilter → extract tenantId
  │
  ├─ WorkflowController.publish()
  │    ├─ parseEtag(ifMatch) → 428 if missing, 400 if malformed
  │    └─ service.publish(tenantId, id, expectedLockVersion)
  │         └─ WorkflowService.publish()
  │              └─ repository.publish(tenantId, id, expectedLockVersion)
  │                   └─ JdbcWorkflowRepository.publish() [@Transactional]
  │                        ├─ lockAndCheck()
  │                        │    ├─ SELECT ... FOR UPDATE
  │                        │    ├─ Check ACTIVE + tenant → 404 if not found
  │                        │    └─ Check lock_version → 409 if mismatch
  │                        ├─ UPDATE workflow_version SET status='PUBLISHED', published_at=NOW()
  │                        │    └─ 0 rows changed → 409 (no draft to publish)
  │                        ├─ incrementVersion() → lock_version + 1
  │                        └─ findById() → read back
  │
  ├─ WorkflowResponse.from(definition) → HTTP DTO
  │
  └─ Return 200 OK + ETag: "1"
```
