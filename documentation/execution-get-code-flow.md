# Get Execution by ID — Complete Code Flow

> `GET /api/v1/executions/{executionId}` — Retrieves full execution details including all task runs, attempts, and events.

---

## Step 1: HTTP Request

```
GET /api/v1/executions/660e8400-e29b-41d4-a716-446655440001
Authorization: Bearer <JWT>
```

No `If-Match` header required — this is a read operation.

---

## Step 2: TenantContextFilter — Extract Tenant Identity

Same as all endpoints.

---

## Step 3: WorkflowExecutionController.get()

**File:** `flowforge-control-plane/.../adapter/in/web/WorkflowExecutionController.java` (line 66–71)

```java
@GetMapping("/executions/{executionId}")
WorkflowExecutionResponse get(HttpServletRequest request, @PathVariable UUID executionId) {
    return WorkflowExecutionResponse.from(
            service.get(TenantContextFilter.requireTenant(request), executionId)
    );
}
```

**What happens:**
1. `@PathVariable UUID executionId` — Spring extracts the UUID from the URL.
2. `service.get(tenantId, executionId)` — Delegates to the application service.
3. Wraps the `WorkflowExecution` domain object in `WorkflowExecutionResponse.from()`.

---

## Step 4: WorkflowExecutionService.get()

**File:** `flowforge-application/.../execution/WorkflowExecutionService.java` (line 122–125)

```java
public WorkflowExecution get(TenantId tenantId, UUID executionId) {
    return repository.findById(Objects.requireNonNull(tenantId), executionId)
            .orElseThrow(() -> new ExecutionNotFoundException(executionId));
}
```

**What happens:**
1. Validates `tenantId` is not null.
2. Calls `repository.findById(tenantId, executionId)`.
3. If the execution doesn't exist (or belongs to a different tenant), `Optional` is empty → throws `ExecutionNotFoundException` → `404 Not Found`.

---

## Step 5: ExecutionRepository.findById() → JdbcExecutionRepository

**File:** `flowforge-control-plane/.../adapter/out/persistence/JdbcExecutionRepository.java`

The `findById()` method loads the **full execution graph** — the execution row, all task runs, all task attempts, and all execution events. This is the "heavy" query compared to the lightweight `ExecutionSummary` used in the list endpoint.

**What is loaded:**

### 5a. Execution Row
```sql
SELECT e.*, w.id AS workflow_id, w.lock_version, ...
  FROM workflow_execution e
  JOIN workflow_version v ON e.workflow_version_id = v.id
  JOIN workflow_definition w ON v.workflow_id = w.id
 WHERE e.id = :executionId
   AND e.tenant_id = :tenantId
```

### 5b. Task Runs
```sql
SELECT id, task_key, status, state_version, created_at, started_at, finished_at
  FROM task_run
 WHERE workflow_execution_id = :executionId
 ORDER BY created_at
```

Each `TaskRun` includes:
- `id` — UUID of the task run
- `taskKey` — the task definition key (e.g., `VALIDATE_ORDER`)
- `status` — `PENDING`, `READY`, `DISPATCHED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `SKIPPED`
- `stateVersion` — optimistic locking version for this task run
- `createdAt`, `startedAt`, `finishedAt` — lifecycle timestamps

### 5c. Task Attempts
```sql
SELECT id, task_run_id, attempt_number, status, started_at, finished_at,
       error_code, error_message
  FROM task_attempt
 WHERE task_run_id IN (SELECT id FROM task_run WHERE workflow_execution_id = :executionId)
 ORDER BY task_run_id, attempt_number
```

Each `TaskAttempt` includes:
- `attemptNumber` — 1-based retry count (1 = first attempt)
- `status` — `RUNNING`, `SUCCEEDED`, `FAILED`
- `errorCode` / `errorMessage` — populated if the attempt failed

### 5d. Execution Events
```sql
SELECT id, task_run_id, type, from_status, to_status, occurred_at
  FROM execution_event
 WHERE workflow_execution_id = :executionId
 ORDER BY occurred_at
```

Events are an audit log of state transitions — e.g., `TASK_DISPATCHED` (READY → DISPATCHED), `TASK_COMPLETED` (RUNNING → SUCCEEDED), `EXECUTION_STARTED` (PENDING → RUNNING), etc.

**Why `@Transactional(readOnly = true)`:** Read-only — no locks, consistent snapshot of the execution state.

---

## Step 6: WorkflowExecutionResponse.from() — Domain → HTTP DTO

**File:** `flowforge-control-plane/.../adapter/in/web/WorkflowExecutionResponse.java` (line 29–45)

```java
public static WorkflowExecutionResponse from(WorkflowExecution execution) {
    var workflow = execution.workflow();
    return new WorkflowExecutionResponse(
            workflow.id(),
            execution.tenantId().value(),
            workflow.workflowId(),
            workflow.workflowVersion(),
            workflow.status(),
            workflow.stateVersion(),
            workflow.createdAt(),
            workflow.startedAt(),
            workflow.finishedAt(),
            execution.tasks().stream().map(TaskResponse::from).toList(),
            execution.attempts().stream().map(AttemptResponse::from).toList(),
            execution.events().stream().map(EventResponse::from).toList()
    );
}
```

The response includes three nested arrays:
- **`tasks`** — one `TaskResponse` per task run (status, timestamps)
- **`attempts`** — one `AttemptResponse` per task attempt (retry number, error details)
- **`events`** — one `EventResponse` per state transition (audit trail)

---

## Step 7: HTTP Response

```
HTTP/1.1 200 OK
Content-Type: application/json

{
  "id": "660e8400-...",
  "tenantId": "tenant-123",
  "workflowId": "550e8400-...",
  "workflowVersion": 1,
  "status": "RUNNING",
  "stateVersion": 5,
  "createdAt": "2026-09-15T12:00:00Z",
  "startedAt": "2026-09-15T12:00:01Z",
  "finishedAt": null,
  "tasks": [
    {
      "id": "task-run-uuid-1",
      "taskKey": "START",
      "status": "SUCCEEDED",
      "stateVersion": 2,
      "createdAt": "2026-09-15T12:00:00Z",
      "startedAt": "2026-09-15T12:00:01Z",
      "finishedAt": "2026-09-15T12:00:01Z"
    },
    {
      "id": "task-run-uuid-2",
      "taskKey": "VALIDATE_ORDER",
      "status": "RUNNING",
      "stateVersion": 1,
      "createdAt": "2026-09-15T12:00:00Z",
      "startedAt": "2026-09-15T12:00:01Z",
      "finishedAt": null
    }
  ],
  "attempts": [
    {
      "id": "attempt-uuid-1",
      "taskId": "task-run-uuid-1",
      "attemptNumber": 1,
      "status": "SUCCEEDED",
      "startedAt": "2026-09-15T12:00:01Z",
      "finishedAt": "2026-09-15T12:00:01Z",
      "errorCode": null,
      "errorMessage": null
    }
  ],
  "events": [
    {
      "id": "event-uuid-1",
      "taskId": null,
      "type": "EXECUTION_STARTED",
      "fromStatus": "PENDING",
      "toStatus": "RUNNING",
      "occurredAt": "2026-09-15T12:00:01Z"
    },
    {
      "id": "event-uuid-2",
      "taskId": "task-run-uuid-1",
      "type": "TASK_DISPATCHED",
      "fromStatus": "READY",
      "toStatus": "DISPATCHED",
      "occurredAt": "2026-09-15T12:00:01Z"
    },
    {
      "id": "event-uuid-3",
      "taskId": "task-run-uuid-1",
      "type": "TASK_COMPLETED",
      "fromStatus": "RUNNING",
      "toStatus": "SUCCEEDED",
      "occurredAt": "2026-09-15T12:00:01Z"
    }
  ]
}
```

---

## Call Chain Summary

```
GET /api/v1/executions/{executionId}
  │
  ├─ TenantContextFilter → extract tenantId
  │
  ├─ WorkflowExecutionController.get()
  │    └─ service.get(tenantId, executionId)
  │         └─ WorkflowExecutionService.get()
  │              ├─ Objects.requireNonNull(tenantId)
  │              └─ repository.findById(tenantId, executionId)
  │                   └─ JdbcExecutionRepository.findById() [@Transactional(readOnly)]
  │                        ├─ SELECT workflow_execution + JOIN workflow_version/definition
  │                        ├─ SELECT task_run × N
  │                        ├─ SELECT task_attempt × M
  │                        └─ SELECT execution_event × E
  │              └─ orElseThrow → ExecutionNotFoundException (404)
  │
  ├─ WorkflowExecutionResponse.from(execution) → HTTP DTO
  │    ├─ tasks[] → TaskResponse[]
  │    ├─ attempts[] → AttemptResponse[]
  │    └─ events[] → EventResponse[]
  │
  └─ Return 200 OK
```
