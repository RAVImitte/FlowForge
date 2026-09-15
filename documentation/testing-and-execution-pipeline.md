# Testing Flow & Execution Pipeline — 10,000 Workflows × 10 Tasks

> How the load test framework works, the end-to-end execution pipeline for 10k workflows with 10 tasks each, and why 4 workers + 12 Kafka partitions is the right topology.

---

## Table of Contents

1. [Testing Flow — How the Load Test Framework Works](#1-testing-flow--how-the-load-test-framework-works)
2. [Execution Pipeline for 10,000 × 10](#2-execution-pipeline-for-10000--10)
3. [Why 4 Workers and 12 Kafka Partitions?](#3-why-4-workers-and-12-kafka-partitions)

---

## 1. Testing Flow — How the Load Test Framework Works

### 1.1 Components

The load testing framework consists of:

| Component | File | Role |
|---|---|---|
| `FlowForgeLoadGenerator` | `flowforge-load-test/.../FlowForgeLoadGenerator.java` | Main entry point — orchestrates the test run |
| `WorkloadProfile` | `flowforge-load-test/.../WorkloadProfile.java` | JSON-loaded profile defining operations, arrival rate, fan-out, thresholds |
| `FlowForgeClient` | `flowforge-load-test/.../FlowForgeClient.java` | HTTP client that creates workflows, starts executions, polls for completion |
| `Accumulator` | `flowforge-load-test/.../Accumulator.java` | Collects per-operation results, computes percentiles, evaluates thresholds |
| `Topology Runner` | `scripts/run-load-topology.ps1` | PowerShell script that spins up Docker Compose topology, runs the test, tears down |

### 1.2 Test Execution Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                    TOPOLOGY RUNNER (PowerShell)                     │
│                                                                     │
│  1. docker compose up (PostgreSQL, Kafka, Control Plane, Workers)  │
│  2. Wait for health checks                                         │
│  3. java -jar flowforge-load-test.jar --profile=phase8-10k-x10     │
│  4. Monitor process exit code                                      │
│  5. Run durable reconciliation query                               │
│  6. docker compose down                                            │
│  7. Report PASS/FAIL                                               │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  FLOWFORGE LOAD GENERATOR (Java)                    │
│                                                                     │
│  ┌─────────────┐    ┌─────────────┐    ┌──────────────────────────┐  │
│  │  Profile    │───▶│  Paced      │───▶│  Virtual Thread Pool    │  │
│  │  Loader     │    │  Submitter  │    │  (one thread per op)    │  │
│  └─────────────┘    └─────────────┘    └──────────────────────────┘  │
│                                              │                      │
│                                              ▼                      │
│                                     ┌────────────────┐               │
│                                     │ FlowForgeClient│               │
│                                     │  (HTTP client) │               │
│                                     └────────────────┘               │
│                                              │                      │
│                                              ▼                      │
│                                     ┌────────────────┐               │
│                                     │  Accumulator   │               │
│                                     │  (results)     │               │
│                                     └────────────────┘               │
│                                              │                      │
│                                              ▼                      │
│                                     ┌────────────────┐               │
│                                     │  Report        │               │
│                                     │  (JSON + exit) │               │
│                                     └────────────────┘               │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.3 Step-by-Step Test Flow

#### Step 1: Load Profile

```java
WorkloadProfile profile = WorkloadProfile.load(Path.of("phase8-10k-x10.json"), objectMapper);
```

The profile is validated at construction time (see `WorkloadProfile` record). All fields are checked — `operations` between 1 and 1M, `fanOut` between 1 and 998, `arrivalRatePerSecond` positive, etc.

#### Step 2: Create and Publish Workflow

Before the test starts, `FlowForgeClient.createAndPublishWorkflow()`:
1. `POST /api/v1/workflows` — Creates a workflow with `fanOut + 2` tasks (START + N fan-out DELAY tasks + JOIN)
2. `POST /api/v1/workflows/{id}/publish` — Publishes it

The DAG structure:
```
         START (NOOP)
        /  /  /  ...  \  \
      FAN_001 ... FAN_010 (DELAY, 25ms each)
        \  \  \  ...  /  /
         JOIN (NOOP)
```

#### Step 3: Paced Submission

```java
long spacingNanos = (long) (1_000_000_000.0 / profile.arrivalRatePerSecond());
Semaphore inFlight = new Semaphore(profile.maxInFlight());

for (int sequence = 0; sequence < profile.operations(); sequence++) {
    long startedNanos = System.nanoTime();
    long targetNanos = startedNanos + spacingNanos * sequence;
    waitUntil(targetNanos);  // precise pacing

    inFlight.acquire();  // backpressure
    Thread.startVirtualThread(() -> {
        try {
            runOperation(sequence, profile, client, accumulator);
        } finally {
            inFlight.release();
        }
    });
}
```

- **Pacing:** `spacingNanos = 1e9 / 5.7 ≈ 175ms` between operation starts. The generator uses `waitUntil()` to maintain precise pacing — it's not "fire as fast as possible."
- **Backpressure:** `Semaphore(250)` limits concurrent in-flight operations. When all 250 permits are taken, the generator blocks before submitting the next.
- **Virtual threads:** Each operation runs on its own virtual thread (Java 21), enabling 10,000 concurrent operations without OS thread overhead.

#### Step 4: Per-Operation Flow

Each virtual thread executes `runOperation()`:

```
1. POST /api/v1/workflows/{id}/executions
   ├─ Idempotency-Key: "op-{sequence}"
   ├─ Measure: admission latency (time from POST to 202 response)
   └─ If 202 Accepted → record "accepted"
       If 429/5xx → record "rejected"

2. Poll GET /api/v1/executions/{executionId}
   ├─ Sleep pollIntervalMs (500ms) between polls
   ├─ Check status: SUCCEEDED → record "succeeded"
   │                 FAILED → record "failed"
   │                 CANCELLED → record "cancelled"
   └─ Timeout after operationTimeoutSeconds (300s) → record "timedOut"

3. Measure: completion latency (time from 202 to terminal state)
```

#### Step 5: Accumulate Results

The `Accumulator` collects:
- `attempted` — total operations submitted
- `accepted` — received 202
- `succeeded` — reached SUCCEEDED
- `failed` — reached FAILED
- `cancelled` — reached CANCELLED
- `timedOut` — didn't reach terminal within timeout
- `unexpected5xx` — count of 5xx HTTP responses
- Admission latencies (for percentile calculation)
- Completion latencies (for percentile calculation)

#### Step 6: Evaluate Thresholds and Report

After all operations complete (or timeout), the `Accumulator.report()` method:

1. Computes ratios:
   - `acceptanceRatio = accepted / attempted`
   - `successRatio = succeeded / accepted`
   - `errorRatio = (attempted - succeeded) / attempted`

2. Computes percentiles (p50, p95, p99) for admission and completion latencies

3. Evaluates all thresholds:
   - `minimumAcceptanceRatio >= 1.0` → every operation must be accepted
   - `minimumSuccessRatio >= 1.0` → every accepted execution must succeed
   - `maximumErrorRatio <= 0.0` → zero errors
   - `maximumStartP95Ms <= 150` → p95 admission latency ≤ 150ms
   - `maximumCompletionP95Ms <= 5000` → p95 completion latency ≤ 5s
   - `unexpected5xx == 0` → no server errors

4. Writes JSON report to stdout and file

5. Exits with code 0 (PASS) or 1 (FAIL)

#### Step 7: Topology Runner Post-Check

The PowerShell topology runner does additional checks:
- **Process exit code** — must be 0
- **Durable reconciliation** — queries the database to verify all executions reached terminal states (no orphaned RUNNING executions)
- **Worker logs** — checks for unhandled exceptions

A run passes **only when all thresholds, process exit, and durable reconciliation all pass**.

---

## 2. Execution Pipeline for 10,000 × 10

### 2.1 The Numbers

| Metric | Value |
|---|---|
| Workflows submitted | 10,000 |
| Tasks per workflow | 12 (START + 10 DELAY + JOIN) |
| Total tasks | 120,000 |
| Arrival rate | 5.7 ops/sec (~30 min run) |
| Max in-flight | 250 concurrent executions |
| Task delay | 25ms per DELAY task |
| Workers | 4 |
| Kafka partitions | 12 |

### 2.2 End-to-End Pipeline

```
CLIENT                    CONTROL PLANE                    KAFKA                    WORKERS
──────                    ─────────────                    ─────                    ───────

POST /executions
     │
     ▼
┌─────────────────┐
│ Admission Gate  │  ← Semaphore limits concurrent starts
│ (acquire lease) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ WorkflowExec    │  ← Check workflow is PUBLISHED + ACTIVE
│ Service.start() │  ← Check idempotency key
│                 │  ← INSERT workflow_execution (PENDING)
│                 │  ← INSERT task_run × 12 (PENDING)
│                 │  ← Mark START task as READY
│                 │  ← INSERT outbox entry
└────────┬────────┘
         │ @Transactional commit
         ▼
┌─────────────────┐
│ OutboxPublisher │  ← Polls outbox table every 100ms
│ (scheduled)     │  ← SELECT ... FOR UPDATE SKIP LOCKED
│                 │  ← Publish to Kafka topic "task-dispatch"
│                 │  ← DELETE outbox row
└────────┬────────┘
         │
         ▼
    ┌────────────────────────────────────────────┐
    │         KAFKA TOPIC: task-dispatch         │
    │                                            │
    │   P0   P1   P2   P3   P4   P5             │
    │   P6   P7   P8   P9   P10  P11            │
    │                                            │
    │   Key: tenantId (ensures same-tenant       │
    │         tasks go to same partition)        │
    └────────────────────┬───────────────────────┘
                         │
          ┌──────────────┼──────────────┐──────────────┐
          ▼              ▼              ▼              ▼
     ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐
     │ Worker 1│   │ Worker 2│   │ Worker 3│   │ Worker 4│
     │         │   │         │   │         │   │         │
     │ P0,P1,P2│   │ P3,P4,P5│   │ P6,P7,P8│   │P9,P10,P11│
     │         │   │         │   │         │   │         │
     │ Consume │   │ Consume │   │ Consume │   │ Consume │
     │ task    │   │ task    │   │ task    │   │ task    │
     │         │   │         │   │         │   │         │
     │ Execute │   │ Execute │   │ Execute │   │ Execute │
     │ DELAY   │   │ DELAY   │   │ DELAY   │   │ DELAY   │
     │ 25ms    │   │ 25ms    │   │ 25ms    │   │ 25ms    │
     │         │   │         │   │         │   │         │
     │ Publish │   │ Publish │   │ Publish │   │ Publish │
     │ result  │   │ result  │   │ result  │   │ result  │
     └────┬────┘   └────┬────┘   └────┬────┘   └────┬────┘
          │              │              │              │
          └──────────────┴──────┬───────┴──────────────┘
                                 │
                                 ▼
                    ┌─────────────────────┐
                    │ KAFKA TOPIC:         │
                    │ task-result          │
                    │ (12 partitions)      │
                    └─────────┬───────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │ Control Plane        │
                    │ ResultConsumer       │
                    │                     │
                    │ completeTask():      │
                    │  ← UPDATE task_run   │
                    │    SET status=       │
                    │    SUCCEEDED         │
                    │  ← Check DAG: are    │
                    │    dependents ready? │
                    │  ← If yes: mark      │
                    │    READY + outbox    │
                    │  ← If all tasks      │
                    │    done: mark         │
                    │    execution          │
                    │    SUCCEEDED         │
                    └─────────────────────┘
```

### 2.3 Task Lifecycle (per task)

```
PENDING → READY → DISPATCHED → RUNNING → SUCCEEDED
                                    │
                                    └→ FAILED (if task fails)
                                         │
                                         └→ RETRY (if maxAttempts > 1)
                                              │
                                              └→ SUCCEEDED or FAILED
```

1. **PENDING** — Task run created, waiting for dependencies to complete
2. **READY** — All dependencies satisfied, task is eligible for dispatch
3. **DISPATCHED** — Task claimed by dispatcher, message sent to Kafka
4. **RUNNING** — Worker picked up the message, executing the task
5. **SUCCEEDED** — Worker completed the task, result written to DB
6. **FAILED** — Task failed (after all retry attempts exhausted)

### 2.4 DAG Advancement

When a task completes (SUCCEEDED), the `completeTask()` method in `JdbcExecutionRepository`:

1. Updates the task_run status to SUCCEEDED
2. Checks all dependents of this task (tasks that depend on it)
3. For each dependent, checks if ALL its prerequisites are now SUCCEEDED
4. If yes → marks the dependent as READY and writes an outbox entry
5. If all tasks in the execution are SUCCEEDED → marks the execution as SUCCEEDED

For the 10k×10 DAG:
- START completes → 10 DELAY tasks become READY (all depend on START)
- 10 DELAY tasks complete (in parallel, 25ms each) → JOIN becomes READY
- JOIN completes → execution is SUCCEEDED

**Critical path per workflow:** START (≈1ms) + max(10 × 25ms in parallel) + JOIN (≈1ms) ≈ **27ms**

**Throughput:** 5.7 ops/sec × 12 tasks = ~68 tasks/sec sustained. With 4 workers, each worker handles ~17 tasks/sec. At 25ms per task, each worker can process 40 tasks/sec — well within capacity.

---

## 3. Why 4 Workers and 12 Kafka Partitions?

### 3.1 The Partition-Worker Relationship

Kafka partitions are the unit of parallelism. Each partition is consumed by exactly one worker within a consumer group. With 12 partitions and 4 workers:

```
Worker 1 → Partitions 0, 1, 2    (3 partitions)
Worker 2 → Partitions 3, 4, 5    (3 partitions)
Worker 3 → Partitions 6, 7, 8    (3 partitions)
Worker 4 → Partitions 9, 10, 11  (3 partitions)
```

Each worker gets 3 partitions, providing 3-way parallelism per worker (Kafka allows one thread per partition within a consumer).

### 3.2 Why 12 Partitions (Not 4, Not 48)?

**Lower bound: ≥ number of workers**
- With 4 partitions and 4 workers, each worker gets 1 partition. If a worker is slow, its partition backs up with no parallelism within the worker.
- 12 partitions give each worker 3 partitions — 3× intra-worker parallelism.

**Upper bound: diminishing returns**
- More partitions = more Kafka overhead (metadata, leader election, storage)
- More partitions than needed = uneven load distribution (some partitions idle)
- 48 partitions with 4 workers = 12 per worker — excessive for 68 tasks/sec

**The 3:1 ratio (partitions:workers)**
- Provides headroom for partition rebalancing during worker restarts
- Allows horizontal scaling: if you add 2 more workers (6 total), each gets 2 partitions — no repartitioning needed
- Matches the typical Kafka best practice of 2–4 partitions per consumer

**Partitioning key: `tenantId`**
- All tasks for the same tenant go to the same partition → preserves per-tenant ordering
- With a single tenant (load test), all tasks go to one partition → **this is why 12 partitions matter**: the single-tenant load is spread across 12 partitions by round-robin (Kafka's default when no key, or sticky partitioning)

### 3.3 Why 4 Workers (Not 1, Not 16)?

**Throughput math:**
```
Required throughput: 68 tasks/sec (5.7 ops/sec × 12 tasks)
Task execution time: 25ms (DELAY task)
Single-worker capacity: 1000ms / 25ms = 40 tasks/sec (single-threaded)
                         40 × 3 (partitions) = 120 tasks/sec (with 3 partitions)

4 workers × 120 = 480 tasks/sec capacity vs. 68 required → 7× headroom
```

**Why not 1 worker?**
- 1 worker × 3 partitions = 120 tasks/sec — technically sufficient for 68/sec
- But: no redundancy. If the worker restarts, all task processing stops.
- GC pauses on a single worker cause latency spikes across the entire system.

**Why not 16 workers?**
- 12 partitions / 16 workers = 4 workers get 0 partitions (idle)
- Wasted resources — Docker containers, memory, Kafka consumer connections
- More workers = more rebalancing overhead during deploys

**Why 4 specifically?**
- 4 workers × 3 partitions each = 12 partitions (clean 1:3 ratio)
- 4 workers provide N+1 redundancy — if 1 worker fails, 3 remain (75% capacity)
- 4 workers fit comfortably on a single machine for development/testing (each worker ~512MB heap)
- 480 tasks/sec capacity vs. 68 required = 7× headroom for retries, GC pauses, and burst traffic

### 3.4 What Happens Under Load?

With 10,000 workflows at 5.7 ops/sec and 250 max in-flight:

```
Time 0:00  ── First 250 operations submitted (max in-flight reached)
Time 0:00  ── Workers start consuming tasks immediately
Time 0:00  ── Each worker processes ~17 tasks/sec (68/4)
Time 0:04  ── First executions complete (~27ms critical path)
Time 0:04  ── In-flight permits released, new operations submitted
Time 29:14  ── Last operation submitted (10000 / 5.7 ≈ 1754 sec)
Time ~29:20  ── Last execution completes (in-flight drain)
```

**Steady state:**
- ~250 concurrent executions in flight
- ~250 × 12 = 3,000 task runs in various states
- ~68 tasks/sec flowing through the pipeline
- Each worker: ~17 tasks/sec, each taking 25ms → 425ms of work per second → 42.5% CPU utilization
- Kafka: 68 messages/sec in + 68 messages/sec out = 136 messages/sec (trivial for Kafka)

**Backpressure points:**
1. **Admission gate** — limits concurrent `POST /executions` to prevent DB write overload
2. **Token bucket rate limiter** — throttles task dispatch if tenant exceeds rate limit
3. **Max in-flight semaphore** — client-side backpressure (250 concurrent operations)
4. **Kafka consumer lag** — if workers can't keep up, messages accumulate in partitions

### 3.5 Scaling Scenarios

| Scenario | Action | Effect |
|---|---|---|
| Double throughput (136 tasks/sec) | Add 2 workers (6 total) | Each worker still gets 2 partitions, capacity = 720/sec |
| 10× throughput (680 tasks/sec) | Increase partitions to 24, workers to 8 | 3 partitions/worker, capacity = 960/sec |
| Worker crash | Kafka rebalances 3 partitions to remaining 3 workers | Each gets 4 partitions, capacity drops to 480/sec (still 7× headroom) |
| Long-running tasks (500ms) | Increase workers to 8 | 8 workers × 3 partitions × 2 tasks/sec = 48/sec capacity per worker → 384/sec total |

### 3.6 Summary: The 4/12 Topology

```
┌──────────────────────────────────────────────────────────┐
│                    4 Workers / 12 Partitions              │
│                                                          │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐
│  │  Worker 1  │  │  Worker 2  │  │  Worker 3  │  │  Worker 4  │
│  │  P0, P1, P2│  │  P3, P4, P5│  │  P6, P7, P8│  │P9,P10,P11 │
│  └────────────┘  └────────────┘  └────────────┘  └────────────┘
│                                                          │
│  Capacity:    480 tasks/sec  (7× headroom over 68/sec)   │
│  Redundancy:  N+1 (survives 1 worker loss)              │
│  Partitions:  3 per worker (intra-worker parallelism)    │
│  Scaling:     Add workers up to 12 (1 partition each)    │
│               without repartitioning                     │
└──────────────────────────────────────────────────────────┘
```

---

*See also: [execution-start-code-flow.md](execution-start-code-flow.md) for the start execution code flow, [ADR-003](../docs/adr/003-phase-3-distributed-execution.md) for the distributed execution architecture decisions.*
