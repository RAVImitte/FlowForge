# Start Execution — Complete Code Flow

> `POST /api/v1/workflows/{workflowId}/executions` — Starts a new workflow execution. Returns `202 Accepted` with the execution ID.

---

## Step 1: HTTP Request

```
POST /api/v1/workflows/550e8400-e29b-41d4-a716-446655440000/executions
Idempotency-Key: client-generated-unique-key-123
Authorization: Bearer <JWT>
```

The `Idempotency-Key` header is required — it prevents duplicate executions if the client retries.

---

## Step 2: TenantContextFilter — Extract Tenant Identity

Same as all endpoints.

---

## Step 3: WorkflowExecutionController.start()

**File:** `flowforge-control-plane/.../adapter/in/web/WorkflowExecutionController.java` (line 34–48)

```java
@PostMapping("/workflows/{workflowId}/executions")
ResponseEntity<WorkflowExecutionResponse> start(
        HttpServletRequest request,
        @PathVariable UUID workflowId,
        @RequestHeader(name = "Idempotency-Key", required = false) String idempotencyKey
) {
    try (WorkflowStartAdmissionGate.Lease ignored = admissionGate.acquire()) {
        WorkflowExecutionResponse response = WorkflowExecutionResponse.from(
                service.start(TenantContextFilter.requireTenant(request), workflowId, idempotencyKey)
        );
        return ResponseEntity.accepted()
                .location(URI.create("/api/v1/executions/" + response.id()))
                .body(response);
    }
}
```

**What happens:**
1. `admissionGate.acquire()` — The `WorkflowStartAdmissionGate` is a semaphore-based admission controller. It limits how many concurrent `start` operations are in-flight. The `try-with-resources` ensures the lease is released when the method returns (even on exception).
2. `service.start(tenantId, workflowId, idempotencyKey)` — Delegates to the application service.
3. Returns `202 Accepted` with `Location` header pointing to the execution resource.

**Why the admission gate?** Under heavy load, unbounded concurrent `start` calls could overwhelm the database with transactional writes (inserting execution rows, task runs, outbox entries). The admission gate applies backpressure — if too many starts are in-flight, new requests block (or time out) rather than cascading failures.

---

## Step 4: WorkflowExecutionService.start()

**File:** `flowforge-application/.../execution/WorkflowExecutionService.java` (line 112–120)

```java
public WorkflowExecution start(TenantId tenantId, UUID workflowId, String idempotencyKey) {
    Objects.requireNonNull(tenantId, "tenantId must not be null");
    Objects.requireNonNull(workflowId, "workflowId must not be null");
    String normalizedKey = normalizeIdempotencyKey(idempotencyKey);
    WorkflowExecution execution = repository.start(tenantId, workflowId, normalizedKey, clock.instant());
    if (!dispatchOnStart) return execution;
    dispatchReadyTasks(tenantId, DEFAULT_DISPATCH_BATCH);
    return repository.findById(tenantId, execution.workflow().id()).orElse(execution);
}
```

**What happens:**
1. `normalizeIdempotencyKey(idempotencyKey)` — Validates and normalizes the key:
   - Must not be blank → `IllegalArgumentException` → `400 Bad Request`
   - Must not exceed 200 characters → `400 Bad Request`
   - Strips leading/trailing whitespace
2. `repository.start(tenantId, workflowId, normalizedKey, clock.instant())` — Creates the execution in the database (see Step 5).
3. `dispatchReadyTasks(tenantId, DEFAULT_DISPATCH_BATCH)` — Immediately dispatches ready tasks (the START task and any tasks with no dependencies). `DEFAULT_DISPATCH_BATCH = 100`.
4. `repository.findById(...)` — Re-reads the execution to get the latest state (tasks may have already been dispatched/claimed).

### `normalizeIdempotencyKey()`

```java
private static String normalizeIdempotencyKey(String value) {
    if (value == null || value.isBlank()) {
        throw new IllegalArgumentException("Idempotency-Key must not be blank");
    }
    String normalized = value.strip();
    if (normalized.length() > 200) {
        throw new IllegalArgumentException("Idempotency-Key must not exceed 200 characters");
    }
    return normalized;
}
```

**Why idempotency?** If the client sends the same request twice (network timeout, retry), the second call must not create a duplicate execution. The repository's `start()` method uses the idempotency key to deduplicate — if a key was already used, it returns the existing execution instead of creating a new one.

---

## Step 5: ExecutionRepository.start() → JdbcExecutionRepository

**File:** `flowforge-control-plane/.../adapter/out/persistence/JdbcExecutionRepository.java`

The `start()` method in the JDBC repository performs a complex transactional operation:

1. **Look up the published workflow version** — Only PUBLISHED workflows can be executed. If the workflow is DRAFT-only or ARCHIVED, `409 Conflict`.
2. **Check idempotency** — `SELECT ... WHERE idempotency_key = :key AND tenant_id = :tenantId`. If found, return the existing execution.
3. **Create execution row** — `INSERT INTO workflow_execution(...)` with status `PENDING`.
4. **Create task runs** — For each task in the workflow definition, insert a `task_run` row with status `PENDING`.
5. **Identify ready tasks** — Tasks with no dependencies (in-degree 0) are marked `READY`.
6. **Write outbox entry** — Insert a row into the `outbox` table for the task dispatcher to pick up (transactional outbox pattern).
7. **Commit** — All of the above is in a single `@Transactional` block.

**Why the transactional outbox?** Instead of publishing to Kafka directly from the HTTP request handler (which could fail after the DB commit, losing the message), the outbox pattern writes the "dispatch task" intent to the database in the same transaction as the execution creation. A separate `OutboxPublisher` polls the outbox table and publishes to Kafka. This guarantees at-least-once delivery.

---

## Step 6: dispatchReadyTasks()

**File:** `flowforge-application/.../execution/WorkflowExecutionService.java` (line 161–180)

```java
private int dispatchReadyTasks(TenantId tenantId, int limit, Instant now) {
    ReadyQueueSnapshot queue = repository.readyQueue(tenantId, now, limit);
    if (queue == null) queue = ReadyQueueSnapshot.unknown(limit);
    backpressureObserver.readyQueueObserved(queue.depth(), queue.oldestAge());
    int requested = (int) Math.min(limit, queue.depth());
    if (requested == 0) return 0;
    TokenBucketDecision decision = rateLimiter.consume(
            tenantId, "task-dispatch",
            quotaProvider.quotaFor(tenantId).policy().dispatchRateLimit(), requested, now
    );
    if (decision.throttled(requested)) {
        backpressureObserver.taskDispatchThrottled(requested, decision.granted(), decision.retryAfter());
    }
    if (decision.granted() == 0) return 0;
    var workItems = repository.claimReadyTasks(tenantId, decision.granted(), now);
    workItems.forEach(this::dispatch);
    return workItems.size();
}
```

**What happens:**
1. `readyQueue(tenantId, now, limit)` — Queries the ready queue depth and oldest task age for backpressure observation.
2. `rateLimiter.consume(...)` — Token bucket rate limiter. Throttles dispatch if the tenant has exceeded its dispatch rate limit.
3. `claimReadyTasks(tenantId, granted, now)` — Atomically claims ready tasks (sets status from `READY` to `DISPATCHED`) and returns `TaskWorkItem` objects.
4. `dispatch(workItem)` — For each work item, calls `dispatcher.dispatch(workItem)` which sends the task to Kafka.

### `dispatch()` — Async Task Dispatch

```java
private void dispatch(TaskWorkItem workItem) {
    try {
        dispatcher.dispatch(workItem).whenComplete((result, failure) -> {
            if (failure != null) {
                complete(workItem, TaskResult.failed("DISPATCH_FAILURE", rootMessage(failure)));
            } else if (result == null) {
                complete(workItem, TaskResult.failed("EMPTY_RESULT", "Task handler returned no result"));
            } else {
                complete(workItem, result);
            }
        });
    } catch (RuntimeException failure) {
        complete(workItem, TaskResult.failed("DISPATCH_FAILURE", rootMessage(failure)));
    }
}
```

The `dispatcher.dispatch()` returns a `CompletableFuture`. When the worker completes the task (via Kafka result message), the future completes and `complete()` is called, which writes the result to the database via `repository.completeTask()`.

---

## Step 7: HTTP Response

```
HTTP/1.1 202 Accepted
Location: /api/v1/executions/660e8400-e29b-41d4-a716-446655440001
Content-Type: application/json

{
  "id": "660e8400-...",
  "tenantId": "tenant-123",
  "workflowId": "550e8400-...",
  "workflowVersion": 1,
  "status": "RUNNING",
  "stateVersion": 1,
  "createdAt": "2026-09-15T12:00:00Z",
  "startedAt": "2026-09-15T12:00:00Z",
  "finishedAt": null,
  "tasks": [
    {"id": "...", "taskKey": "START", "status": "DISPATCHED", ...},
    {"id": "...", "taskKey": "VALIDATE_ORDER", "status": "PENDING", ...}
  ],
  "attempts": [],
  "events": [...]
}
```

The `202 Accepted` status means "we've accepted your request and started processing, but it's not done yet." The client should poll `GET /api/v1/executions/{id}` to track progress.

---

## Call Chain Summary

```
POST /api/v1/workflows/{workflowId}/executions  (Idempotency-Key: ...)
  │
  ├─ TenantContextFilter → extract tenantId
  │
  ├─ WorkflowExecutionController.start()
  │    ├─ admissionGate.acquire() → backpressure lease (try-with-resources)
  │    └─ service.start(tenantId, workflowId, idempotencyKey)
  │         └─ WorkflowExecutionService.start()
  │              ├─ normalizeIdempotencyKey() → 400 if blank/too long
  │              ├─ repository.start(tenantId, workflowId, key, now)
  │              │    └─ JdbcExecutionRepository.start() [@Transactional]
  │              │         ├─ Look up PUBLISHED workflow version → 409 if not published
  │              │         ├─ Check idempotency key → return existing if duplicate
  │              │         ├─ INSERT workflow_execution (PENDING)
  │              │         ├─ INSERT task_run × N (PENDING)
  │              │         ├─ Mark in-degree-0 tasks as READY
  │              │         └─ INSERT outbox entry (transactional outbox)
  │              ├─ dispatchReadyTasks(tenantId, 100)
  │              │    ├─ readyQueue() → check queue depth
  │              │    ├─ rateLimiter.consume() → token bucket throttle
  │              │    ├─ claimReadyTasks() → atomically claim READY → DISPATCHED
  │              │    └─ dispatch() × N → Kafka publish (async)
  │              └─ repository.findById() → re-read latest state
  │
  ├─ WorkflowExecutionResponse.from(execution) → HTTP DTO
  │
  └─ Return 202 Accepted + Location header
```
