# Cancel Execution — Complete Code Flow

> `POST /api/v1/executions/{executionId}/cancel` — Cancels a running execution. Transitions the execution to `CANCELLED` and marks in-flight tasks as `SKIPPED`.

---

## Step 1: HTTP Request

```
POST /api/v1/executions/660e8400-e29b-41d4-a716-446655440001/cancel
Authorization: Bearer <JWT>
```

No request body and no `If-Match` header required. The `Idempotency-Key` is not required either — cancellation is naturally idempotent (cancelling an already-cancelled execution is a no-op).

---

## Step 2: TenantContextFilter — Extract Tenant Identity

Same as all endpoints.

---

## Step 3: WorkflowExecutionController.cancel()

**File:** `flowforge-control-plane/.../adapter/in/web/WorkflowExecutionController.java` (line 74–79)

```java
@PostMapping("/executions/{executionId}/cancel")
WorkflowExecutionResponse cancel(HttpServletRequest request, @PathVariable UUID executionId) {
    return WorkflowExecutionResponse.from(
            service.cancel(TenantContextFilter.requireTenant(request), executionId)
    );
}
```

**What happens:**
1. `@PathVariable UUID executionId` — Spring extracts the UUID from the URL.
2. `service.cancel(tenantId, executionId)` — Delegates to the application service.
3. Returns the updated execution with `status: "CANCELLED"`.

**Why no `If-Match`?** Unlike workflow mutations (update, publish, archive), execution cancellation doesn't use optimistic locking. This is intentional:
- Executions are transient — they don't have long-lived clients that hold ETags
- Cancellation is a "best effort" operation — if the execution already finished, the cancel is a no-op
- The `stateVersion` on the execution provides concurrency control at the database level

---

## Step 4: WorkflowExecutionService.cancel()

**File:** `flowforge-application/.../execution/WorkflowExecutionService.java` (line 127–131)

```java
public WorkflowExecution cancel(TenantId tenantId, UUID executionId) {
    Objects.requireNonNull(tenantId, "tenantId must not be null");
    Objects.requireNonNull(executionId, "executionId must not be null");
    return repository.cancel(tenantId, executionId, clock.instant());
}
```

**What happens:**
1. Validates `tenantId` and `executionId` are not null.
2. Calls `repository.cancel(tenantId, executionId, clock.instant())` — passes the current time for the `finishedAt` timestamp.
3. Returns the updated `WorkflowExecution`.

---

## Step 5: ExecutionRepository.cancel() → JdbcExecutionRepository

**File:** `flowforge-control-plane/.../adapter/out/persistence/JdbcExecutionRepository.java`

The `cancel()` method performs a transactional state transition:

### 5a. Lock the Execution Row

```sql
SELECT id, status, state_version
  FROM workflow_execution
 WHERE id = :executionId
   AND tenant_id = :tenantId
 FOR UPDATE
```

- `SELECT ... FOR UPDATE` acquires a pessimistic row lock to prevent concurrent cancel/complete operations.
- If the execution doesn't exist or belongs to a different tenant → `ExecutionNotFoundException` → `404 Not Found`.

### 5b. Check Terminal State

If the execution is already in a terminal state (`SUCCEEDED`, `FAILED`, `CANCELLED`, `TIMED_OUT`), cancellation is a **no-op** — the method returns the execution as-is without modification. This makes cancel idempotent.

### 5c. Transition Execution to CANCELLED

```sql
UPDATE workflow_execution
   SET status = 'CANCELLED',
       state_version = state_version + 1,
       finished_at = :now,
       updated_at = CURRENT_TIMESTAMP
 WHERE id = :executionId
   AND tenant_id = :tenantId
```

- Sets `status` to `CANCELLED`
- Increments `state_version` (invalidates any stale reads)
- Sets `finished_at` to the current time

### 5d. Cancel In-Flight Tasks

```sql
UPDATE task_run
   SET status = 'SKIPPED',
       state_version = state_version + 1,
       finished_at = :now,
       updated_at = CURRENT_TIMESTAMP
 WHERE workflow_execution_id = :executionId
   AND status IN ('PENDING', 'READY', 'DISPATCHED', 'RUNNING')
```

- All non-terminal tasks are marked `SKIPPED` (not `FAILED` — they didn't fail, they were abandoned)
- `SKIPPED` is a distinct status that indicates the task was never executed due to cancellation

### 5e. Write Execution Event

```sql
INSERT INTO execution_event(id, workflow_execution_id, task_run_id, type, from_status, to_status, occurred_at)
VALUES (:id, :executionId, NULL, 'EXECUTION_CANCELLED', :fromStatus, 'CANCELLED', :now)
```

- Records the state transition in the audit log
- `task_run_id` is NULL because this is an execution-level event, not a task-level event

### 5f. Read Back

`findById(tenantId, executionId)` — returns the full execution with updated task statuses and the new event.

**Why `@Transactional`:** Steps 5a–5e must be atomic. If the task update fails, the execution status update must roll back. A half-cancelled execution (status CANCELLED but tasks still RUNNING) would be inconsistent.

---

## What Happens to In-Flight Workers?

When a task is `DISPATCHED` or `RUNNING`, a worker may be actively processing it. The cancel operation:

1. **Does NOT send a signal to the worker** — there's no Kafka "cancel" message. The worker will complete the task normally.
2. **The task result is ignored** — when the worker sends the completion result via Kafka, the `completeTask()` method in the repository checks the task's current status. If it's already `SKIPPED`, the result is discarded (the task is no longer `RUNNING`).
3. **The execution is already terminal** — the `CANCELLED` status means no new tasks will be dispatched, even if a dependent task's prerequisite just completed.

This is a **graceful cancellation** — in-flight work is allowed to finish, but its results are discarded, and no new work is started.

---

## Step 6: HTTP Response

```
HTTP/1.1 200 OK
Content-Type: application/json

{
  "id": "660e8400-...",
  "tenantId": "tenant-123",
  "workflowId": "550e8400-...",
  "workflowVersion": 1,
  "status": "CANCELLED",
  "stateVersion": 6,
  "createdAt": "2026-09-15T12:00:00Z",
  "startedAt": "2026-09-15T12:00:01Z",
  "finishedAt": "2026-09-15T12:05:30Z",
  "tasks": [
    {"id": "...", "taskKey": "START", "status": "SUCCEEDED", ...},
    {"id": "...", "taskKey": "VALIDATE_ORDER", "status": "SKIPPED", ...},
    {"id": "...", "taskKey": "PROCESS_PAYMENT", "status": "SKIPPED", ...}
  ],
  "attempts": [...],
  "events": [
    ...,
    {
      "id": "event-uuid-N",
      "taskId": null,
      "type": "EXECUTION_CANCELLED",
      "fromStatus": "RUNNING",
      "toStatus": "CANCELLED",
      "occurredAt": "2026-09-15T12:05:30Z"
    }
  ]
}
```

---

## Call Chain Summary

```
POST /api/v1/executions/{executionId}/cancel
  │
  ├─ TenantContextFilter → extract tenantId
  │
  ├─ WorkflowExecutionController.cancel()
  │    └─ service.cancel(tenantId, executionId)
  │         └─ WorkflowExecutionService.cancel()
  │              ├─ Objects.requireNonNull(tenantId)
  │              ├─ Objects.requireNonNull(executionId)
  │              └─ repository.cancel(tenantId, executionId, now)
  │                   └─ JdbcExecutionRepository.cancel() [@Transactional]
  │                        ├─ SELECT ... FOR UPDATE (lock execution row)
  │                        │    └─ 404 if not found / wrong tenant
  │                        ├─ Check if already terminal → no-op (idempotent)
  │                        ├─ UPDATE workflow_execution
  │                        │    SET status = 'CANCELLED', finished_at = :now
  │                        ├─ UPDATE task_run
  │                        │    SET status = 'SKIPPED'
  │                        │    WHERE status IN ('PENDING','READY','DISPATCHED','RUNNING')
  │                        ├─ INSERT execution_event (EXECUTION_CANCELLED)
  │                        └─ findById() → read back
  │
  ├─ WorkflowExecutionResponse.from(execution) → HTTP DTO
  │
  └─ Return 200 OK
```

---

## Cancellation State Machine

```
                    CANCEL (POST /cancel)
                         │
    ┌────────────────────┼────────────────────┐
    │                    │                    │
    ▼                    ▼                    ▼
  PENDING            RUNNING              SUCCEEDED
    │                    │              (no-op, already
    │                    │               terminal)
    ▼                    ▼
  CANCELLED           CANCELLED
  (tasks SKIPPED)    (in-flight tasks SKIPPED,
                     running tasks allowed to finish
                     but results discarded)
```
