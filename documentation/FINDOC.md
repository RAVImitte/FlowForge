## 1. The Problem FlowForge Solves

**Without a workflow engine**, every business process that involves multiple steps with dependencies is hardcoded into application code. Consider an e-commerce order:

```
Validate Order → Process Payment → Reserve Inventory → Ship → Notify Customer
```

Without FlowForge, a developer would:
1. Write a monolithic function that calls each step sequentially
2. Handle failures with try/catch blocks scattered everywhere
3. Have no visibility into which step failed
4. Have no way to retry just the failed step
5. Have no audit trail of what happened
6. Have no way to scale — if payment takes 5 seconds, the entire thread is blocked
7. Have no way to handle parallel steps (e.g., reserve inventory AND send confirmation email simultaneously)

---

## 2. What FlowForge Provides

| Capability | Without FlowForge | With FlowForge |
|---|---|---|
| **DAG execution** | Hardcoded if/else chains | Define tasks + dependencies as data; engine resolves execution order |
| **Parallelism** | Manual thread management | Fan-out tasks run in parallel automatically (10 DELAY tasks at once) |
| **Retries** | Custom retry logic per step | Per-task reliability policy (maxAttempts, backoff, jitter, retryable error codes) |
| **Observability** | Log scraping | Every task state transition is an event; query `GET /executions/{id}` for full timeline |
| **Scaling** | Vertical (bigger server) | Horizontal — add workers, Kafka partitions scale with you |
| **Fault tolerance** | Process crash = lost work | Transactional outbox + Kafka = at-least-once delivery; worker restart resumes |
| **Multi-tenancy** | Separate deployment per tenant | One deployment, tenant isolation at DB + Kafka level |
| **Versioning** | Deploy = risk | DRAFT → PUBLISHED model; published versions are immutable; test before publish |
| **Cancellation** | Kill the process (ugly) | `POST /cancel` → graceful: in-flight tasks finish, results discarded, no new tasks start |
| **Idempotency** | Duplicate submissions = duplicate side effects | `Idempotency-Key` header prevents duplicate executions |

---

## 3. What Happens Without FlowForge?

### Scenario: Processing 10,000 orders with 10 steps each

#### Without FlowForge (naive approach):

```java
for (Order order : orders) {
    validate(order);           // if this fails, order is lost
    chargePayment(order);       // if this fails, no retry, manual intervention
    reserveInventory(order);    // if this fails, payment already charged — inconsistency
    ship(order);                // sequential, no parallelism
    notify(order);              // if this fails, customer doesn't know
}
```

Problems:
1. **No parallelism** — 10,000 orders × 10 steps × 25ms = 2,500 seconds (42 min) sequentially
2. **No retry** — one network blip on payment = failed order, no automatic recovery
3. **No visibility** — "where is order #5000?" → grep logs
4. **No scaling** — to go faster, buy a bigger server
5. **No fault tolerance** — server crash mid-batch = 5,000 orders in unknown state
6. **No audit trail** — "did we charge this customer?" → check payment system, not your app

#### With FlowForge:

```
POST /api/v1/workflows/{id}/executions  × 10,000
```

Results:
1. **Parallelism** — 10 tasks per workflow run in parallel; 250 workflows in flight simultaneously
2. **Automatic retry** — payment fails → retry with exponential backoff (configurable per task)
3. **Full visibility** — `GET /api/v1/executions/{id}` → see every task, attempt, and event
4. **Horizontal scaling** — add workers, throughput increases linearly
5. **Fault tolerance** — worker crashes → Kafka rebalances → another worker picks up; no tasks lost
6. **Audit trail** — `execution_event` table records every state transition with timestamps

---

## 4. The Core Value Proposition

FlowForge is a **workflow orchestration engine** that turns business processes into:

- **Data** (workflow definitions with DAGs) instead of code
- **Observable** (every state transition is tracked) instead of opaque
- **Resilient** (retries, outbox, Kafka) instead of fragile
- **Scalable** (partition-based parallelism) instead of single-threaded
- **Multi-tenant** (one deployment serves all tenants) instead of siloed

### Key Architectural Decisions

| Decision | Why |
|---|---|
| **Clean / Hexagonal Architecture** | Domain logic is framework-free; swap Spring for Quarkus or PostgreSQL for MySQL without touching invariants |
| **Transactional Outbox Pattern** | Guarantees at-least-once task delivery — no lost messages even if Kafka is down during DB commit |
| **Kafka for task dispatch** | Decouples control plane from workers; enables horizontal scaling, fault tolerance, and ordered per-tenant processing |
| **DRAFT → PUBLISHED versioning** | Published workflow versions are immutable; executions always reference a frozen snapshot |
| **Optimistic locking (ETag/If-Match)** | Prevents lost updates when multiple clients edit the same workflow concurrently |
| **Pessimistic locking (FOR UPDATE)** | Serializes mutations within a transaction; combined with optimistic locking for belt-and-suspenders correctness |
| **Idempotency keys** | Safe client retries — duplicate `POST /executions` with the same key returns the original execution |
| **Admission gate (semaphore)** | Backpressure on `POST /executions` prevents database write overload under burst traffic |
| **Token bucket rate limiter** | Per-tenant dispatch throttling prevents noisy-neighbor problems in multi-tenant deployments |
| **Soft delete (archive)** | No data loss; referential integrity preserved; audit trail retained |

---

## 5. Real-World Analogy

Think of FlowForge as an **assembly line manager** for software processes:

- You define the **blueprint** (workflow definition with task DAG)
- You **publish** the blueprint (immutable version)
- The factory **starts production** (execution)
- Workers **pick up tasks** from the conveyor belt (Kafka)
- The manager **tracks progress** (task states, events)
- If a worker is **sick** (crashes), another takes over
- If a step **fails**, it's **retried** automatically
- You can **cancel** the whole production run gracefully

Without the assembly line manager, every worker would have to figure out what to do next, coordinate with everyone else, handle their own failures, and report to the boss individually — chaos.

---

## When To Use FlowForge

| Use Case | Good Fit? | Why |
|---|---|---|
| Order processing (validate → pay → ship) | ✅ | Multi-step, needs retries, needs audit trail |
| ETL pipeline (extract → transform → load) | ✅ | Parallel transforms, failure recovery, visibility |
| User onboarding (create account → send email → provision resources) | ✅ | Async steps, retry on failure, track progress |
| Simple CRUD API (create/read/update/delete) | ❌ | No multi-step orchestration needed |
| Real-time request/response (search API) | ❌ | Synchronous, no DAG, no retries needed |
| Stream processing (Kafka Streams) | ❌ | Use a stream processor, not a workflow engine |

---
