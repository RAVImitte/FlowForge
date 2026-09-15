# FlowForge — Project Documentation

> A senior-engineer walkthrough of what FlowForge is, why it exists, and what happens under the hood when you use it.

---

## Table of Contents

1. [What is FlowForge?](#1-what-is-flowforge)
2. [Why do we need it?](#2-why-do-we-need-it)
3. [Architecture Overview](#3-architecture-overview)
4. [Module Deep-Dive](#4-module-deep-dive)
   - [4.1 Domain Module](#41-domain-module-flowforge-domain)
   - [4.2 Application Module](#42-application-module-flowforge-application)
   - [4.3 Control-Plane Module](#43-control-plane-module-flowforge-control-plane)
5. [Database Schema](#5-database-schema)
6. [What Happens When You Use It — End-to-End Flows](#6-what-happens-when-you-use-it--end-to-end-flows)
7. [Concurrency Model](#7-concurrency-model)
8. [Version Lifecycle](#8-version-lifecycle)
9. [Error Handling](#9-error-handling)
10. [Testing Strategy](#10-testing-strategy)
11. [How to Run](#11-how-to-run)
12. [Roadmap — Where This Is Going](#12-roadmap--where-this-is-going)

---

## 1. What is FlowForge?

FlowForge is a **distributed workflow and job orchestration platform**. Its job is to let you define, version, and eventually execute multi-step processes as **directed acyclic graphs (DAGs)** of tasks — and to do so with strong correctness guarantee.s

As of **Phase 1** (the current phase), FlowForge is a **workflow-definition control plane**. This means it handles the full lifecycle of *defining* workflows — creating them, validating them, versioning them, publishing them, and archiving them — backed by PostgreSQL. It does **not** yet execute workflows; execution, Kafka-based dispatch, retries, scheduling, and Redis coordination arrive in later phases.

### Phase 1 capabilities at a glance

| Capability | Status |
|---|---|
| Create, read, list, update, publish, and archive workflow definitions | ✅ |
| Model task dependencies as a validated DAG | ✅ |
| Immutable published versions + editable drafts | ✅ |
| Optimistic concurrency via ETags and `If-Match` | ✅ |
| Flyway-managed schema migrations | ✅ |
| Health, metrics, and info actuator endpoints | ✅ |
| Domain, HTTP, architecture, and persistence tests | ✅ |
| Kafka execution, retries, scheduling, workers, Redis | 🔜 Later phases |

---

## 2. Why do we need it?

### The problem

When you orchestrate real business processes — "validate the order, charge the card, notify the customer" — you quickly face a set of hard problems:

1. **Complexity of dependencies.** Tasks have ordering constraints. Some can run in parallel; some must wait. You need a way to express and validate these dependencies, including detecting cycles (which would deadlock execution).

2. **Versioning and immutability.** A workflow definition is not a one-time artifact. It evolves. But once a version is *published* and executions are running against it, that version must be **immutable** — you cannot change the rules mid-flight. You need a draft/publish model.

3. **Concurrent modifications.** Multiple operators or automation systems may try to update the same workflow simultaneously. Without concurrency control, you get **lost updates** — one writer silently overwrites another's changes.

4. **Durability.** Workflow definitions are mission-critical configuration. They must survive restarts and be the durable source of truth.

5. **Future-proofing.** The system will eventually become distributed (Kafka workers, Redis coordination, scheduling). The architecture must be structured so those capabilities can be added *without rewriting the definition model*.

### How FlowForge addresses each

| Problem | FlowForge's answer |
|---|---|
| Dependency complexity | Tasks + dependencies are modeled as a **DAG**, validated at construction time using Kahn's topological-sort algorithm to reject cycles, duplicates, and dangling references. |
| Versioning & immutability | A **draft/publish** model: one editable DRAFT at a time, PUBLISHED versions are immutable and never mutated. A partial unique index in PostgreSQL enforces "one draft per workflow" at the database level. |
| Concurrent modifications | **Optimistic locking** via a `lock_version` column and HTTP ETags. Every mutation requires `If-Match: "<version>"`. Stale writers get `409 Conflict`. |
| Durability | **PostgreSQL** is the source of truth. Schema is managed by **Flyway** migrations. |
| Future-proofing | **Clean architecture** with strict module boundaries enforced by ArchUnit tests. The domain and application modules have zero framework dependencies. Later phases add execution, Kafka, and Redis without touching definition semantics. |

---

## 3. Architecture Overview

FlowForge uses a **Maven multi-module clean architecture** with three modules and a strict dependency rule:

```
┌─────────────────────────────────────────────────────┐
│            flowforge-control-plane                   │
│  (Spring Boot — HTTP adapter + PostgreSQL adapter)  │
│                                                     │
│   ┌─────────────┐         ┌──────────────────────┐  │
│   │ Web Adapter  │         │ Persistence Adapter   │  │
│   │ (REST API)   │         │ (JdbcWorkflowRepo)    │  │
│   └──────┬──────┘         └──────────┬───────────┘  │
│          │                           │              │
├──────────┼───────────────────────────┼──────────────┤
│          ▼                           ▼              │
│  ┌────────────────────────────────────────────────┐│
│  │          flowforge-application                  ││
│  │  (Use cases + repository port)                  ││
│  │  WorkflowService, WorkflowRepository            ││
│  └────────────────────┬───────────────────────────┘│
│                       │                            │
├───────────────────────┼────────────────────────────┤
│                       ▼                            │
│  ┌────────────────────────────────────────────────┐│
│  │          flowforge-domain                       ││
│  │  (Pure Java — invariants & DAG validation)     ││
│  │  WorkflowDraft, WorkflowDefinition, etc.        ││
│  └────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────┘
```

### The dependency rule

```
control-plane  →  application  →  domain
```

Dependencies flow **inward only**. The domain module knows nothing about Spring, JPA, JDBC, HTTP, or JSON. The application module knows nothing about Spring or HTTP. Only the control-plane module touches frameworks.

This is **enforced by an ArchUnit test** (`ArchitectureTest`) that fails the build if any class in `io.flowforge.domain..` or `io.flowforge.application..` depends on `org.springframework..`, `jakarta..`, or `io.flowforge.controlplane..`.

### Why Spring JDBC instead of an ORM?

The ADR-001 explicitly chose `JdbcClient` (Spring JDBC) over an ORM like JPA/Hibernate. The reason: later phases need **explicit control** over locking (`SELECT ... FOR UPDATE`), compare-and-set transitions, and `FOR UPDATE SKIP LOCKED` scheduling queries. An ORM would hide these concurrency primitives behind abstractions, making correctness harder to reason about. Explicit SQL keeps transaction boundaries and locking visible and testable.

---

## 4. Module Deep-Dive

### 4.1 Domain Module (`flowforge-domain`)

**Purpose:** Owns the workflow graph invariants. Pure Java — no framework dependencies (not even Spring at compile scope).

**Package:** `io.flowforge.domain.workflow`

#### Core types

| Type | Kind | Responsibility |
|---|---|---|
| `WorkflowDraft` | `record` | The **input** to create/update. Validates name, description, tasks, and dependencies on construction. This is where DAG validation lives. |
| `WorkflowDefinition` | `record` | The **persisted** workflow — the output of read/create/update/publish. Immutable snapshot with metadata (id, lockVersion, timestamps, version status). |
| `TaskDefinition` | `record` | A single task: `key`, `name`, `type`, `configuration` (a `Map<String, Object>`). |
| `TaskDependency` | `record` | An edge: `taskKey` depends on `dependsOnTaskKey`. |
| `WorkflowVersionStatus` | `enum` | `DRAFT`, `PUBLISHED` |
| `WorkflowLifecycleStatus` | `enum` | `ACTIVE`, `ARCHIVED` |
| `DomainValidationException` | `RuntimeException` | Carries a `List<String>` of all validation violations. |

#### DAG validation in `WorkflowDraft`

When you construct a `WorkflowDraft`, the compact constructor runs a comprehensive validation:

1. **Name checks** — not blank, ≤ 200 characters.
2. **Description checks** — ≤ 2,000 characters.
3. **Task list checks** — at least one task, at most 1,000 tasks.
4. **Per-task checks** — `key` matches `[A-Z][A-Z0-9_]{0,99}`, `name` not blank and ≤ 200 chars, `type` matches `[A-Z][A-Z0-9_.-]{0,99}`.
5. **Duplicate task keys** — rejected.
6. **Dependency checks** — both `taskKey` and `dependsOnTaskKey` must reference existing tasks; no self-dependencies; no duplicate edges.
7. **Cycle detection** — uses **Kahn's algorithm** (topological sort via in-degree counting). If the number of visited nodes ≠ total nodes, a cycle exists and the draft is rejected.

All violations are **collected** (not fail-fast) and thrown together in a single `DomainValidationException`. This gives the API consumer the full list of problems in one response.

#### Kahn's algorithm (simplified)

```
1. Compute in-degree for every task node.
2. Add all zero-in-degree nodes to a queue.
3. While queue is not empty:
   a. Remove a node, increment visited count.
   b. For each dependent of that node, decrement its in-degree.
   c. If a dependent's in-degree reaches 0, add it to the queue.
4. If visited ≠ total nodes → cycle exists.
```

### 4.2 Application Module (`flowforge-application`)

**Purpose:** Owns use cases and the outbound repository port. Depends only on the domain module.

**Package:** `io.flowforge.application.workflow`

#### Core types

| Type | Kind | Responsibility |
|---|---|---|
| `WorkflowService` | `final class` | The application service. Thin orchestrator that delegates to the repository. Validates pagination parameters. |
| `WorkflowRepository` | `interface` | The **port** — a persistence interface that the control-plane module implements. This is the hexagonal "outbound port." |
| `PageResult<T>` | `record` | Generic pagination wrapper: `items`, `page`, `size`, `totalElements`. |
| `WorkflowNotFoundException` | `RuntimeException` | Thrown when a workflow is not found or is archived. |
| `WorkflowConflictException` | `RuntimeException` | Thrown on optimistic-locking failure (stale ETag). |

#### `WorkflowService` methods

| Method | What it does |
|---|---|
| `create(WorkflowDraft)` | Delegates to `repository.create(draft)`. |
| `get(UUID id)` | Finds by ID; throws `WorkflowNotFoundException` if absent. |
| `list(int page, int size)` | Validates page ≥ 0 and 1 ≤ size ≤ 100, then delegates. |
| `update(UUID id, long expectedLockVersion, WorkflowDraft)` | Delegates to repository with optimistic-lock check. |
| `publish(UUID id, long expectedLockVersion)` | Promotes the current DRAFT to PUBLISHED. |
| `archive(UUID id, long expectedLockVersion)` | Soft-deletes (sets lifecycle to ARCHIVED). |

The service is intentionally thin — it validates inputs and delegates. The real logic (locking, version transitions, graph persistence) lives in the repository adapter. This keeps the use cases framework-free and testable.

#### The `WorkflowRepository` port

```java
public interface WorkflowRepository {
    WorkflowDefinition create(WorkflowDraft draft);
    Optional<WorkflowDefinition> findById(UUID id);
    PageResult<WorkflowDefinition> findAll(int page, int size);
    WorkflowDefinition update(UUID id, long expectedLockVersion, WorkflowDraft draft);
    WorkflowDefinition publish(UUID id, long expectedLockVersion);
    void archive(UUID id, long expectedLockVersion);
}
```

This interface is the **contract** between the application layer and the persistence adapter. The domain module doesn't know about it; the application module defines it; the control-plane module implements it.

### 4.3 Control-Plane Module (`flowforge-control-plane`)

**Purpose:** The deployable Spring Boot application. Owns HTTP adapters (inbound) and the PostgreSQL persistence adapter (outbound).

**Package:** `io.flowforge.controlplane`

#### Structure

```
control-plane/
├── FlowForgeApplication.java          ← Spring Boot entry point
├── config/
│   └── ApplicationConfiguration.java  ← Wires WorkflowService with its repository
├── adapter/in/web/                    ← Inbound HTTP adapter
│   ├── WorkflowController.java
│   ├── WorkflowRequest.java
│   ├── WorkflowResponse.java
│   ├── WorkflowPageResponse.java
│   ├── ApiExceptionHandler.java
│   └── PreconditionRequiredException.java
├── adapter/out/persistence/           ← Outbound persistence adapter
│   └── JdbcWorkflowRepository.java
└── resources/
    ├── application.yml
    └── db/migration/
        └── V1__create_workflow_definition_schema.sql
```

#### `WorkflowController` — the REST API

Base path: `/api/v1/workflows`

| HTTP Method | Path | Action | Requires `If-Match`? |
|---|---|---|---|
| `POST` | `/` | Create workflow + draft version 1 | No (new resource) |
| `GET` | `/{id}` | Read current version (draft or latest published) | No |
| `GET` | `/?page=0&size=20` | List active workflows (paginated) | No |
| `PUT` | `/{id}` | Update the draft (or create a new draft if published) | **Yes** |
| `POST` | `/{id}/publish` | Publish the current draft | **Yes** |
| `DELETE` | `/{id}` | Archive (soft delete) | **Yes** |

Every mutating response includes an `ETag` header (e.g., `ETag: "0"`). The next mutation must send it back as `If-Match: "0"`. If the header is missing, the server returns `428 Precondition Required`. If the version doesn't match, it returns `409 Conflict`.

#### Request/response DTOs

- **`WorkflowRequest`** — the inbound DTO with Bean Validation annotations (`@NotBlank`, `@Size`, `@Pattern`, `@NotEmpty`). Contains nested `TaskRequest` and `DependencyRequest` records.
- **`WorkflowResponse`** — the outbound DTO. Has a static `from(WorkflowDefinition)` factory method that maps the domain object to the API representation.
- **`WorkflowPageResponse`** — wraps a page of `WorkflowResponse` items with `totalPages` computed from `totalElements / size`.

The controller converts `WorkflowRequest` → `WorkflowDraft` (domain object) before calling the service, and converts `WorkflowDefinition` (domain object) → `WorkflowResponse` before returning. This keeps the domain model free of HTTP/JSON concerns.

#### `ApiExceptionHandler` — Problem Details (RFC 9457)

A `@RestControllerAdvice` that catches domain and application exceptions and converts them to standard **Problem Detail** responses (`application/problem+json`):

| Exception | HTTP Status | Error code |
|---|---|---|
| `WorkflowNotFoundException` | 404 Not Found | `WORKFLOW_NOT_FOUND` |
| `WorkflowConflictException` | 409 Conflict | `WORKFLOW_CONFLICT` |
| `PreconditionRequiredException` | 428 Precondition Required | `IF_MATCH_REQUIRED` |
| `DomainValidationException` | 422 Unprocessable Content | `INVALID_WORKFLOW` (includes `violations` array) |
| `MethodArgumentNotValidException` | 400 Bad Request | `INVALID_REQUEST` (includes `violations` array) |
| `IllegalArgumentException` | 400 Bad Request | `INVALID_REQUEST` |

Each problem detail includes: `title`, `detail`, `instance` (request URI), and a custom `code` property.

#### `JdbcWorkflowRepository` — the persistence adapter

This is the most substantial class (324 lines). It implements `WorkflowRepository` using Spring's `JdbcClient`. Key behaviors:

- **`create`** — inserts a `workflow_definition` row (ACTIVE, lock_version=0), inserts a `workflow_version` row (version_number=1, DRAFT), then calls `replaceGraph` to insert all tasks and dependencies.
- **`findById`** — uses a `LATERAL` join to pick the "current" version: DRAFT if one exists, otherwise the latest PUBLISHED. Then loads tasks (ordered by position) and dependencies.
- **`findAll`** — counts active definitions, fetches a page of UUIDs, then calls `findById` for each (N+1 pattern — acceptable for Phase 1's scale).
- **`update`** — calls `lockAndCheck` (pessimistic row lock + version check), then either updates the existing DRAFT or creates a new version (if the current draft was published). Replaces the task/dependency graph. Increments `lock_version`.
- **`publish`** — calls `lockAndCheck`, flips the DRAFT to PUBLISHED with a `published_at` timestamp. If no draft exists, throws `WorkflowConflictException`. Increments `lock_version`.
- **`archive`** — calls `lockAndCheck`, sets `lifecycle_status = 'ARCHIVED'`, increments `lock_version`.
- **`lockAndCheck`** — `SELECT ... FOR UPDATE` on the `workflow_definition` row. Checks the row exists, is ACTIVE, and the `lock_version` matches the expected value. Throws `WorkflowNotFoundException` or `WorkflowConflictException` as appropriate.
- **`replaceGraph`** — deletes all tasks and dependencies for a version, then re-inserts them. This is a simple "replace" strategy — delete-then-insert within the transaction.
- **`toJson` / `fromJson`** — serializes/deserializes task `configuration` maps to/from JSONB using Jackson.

#### `ApplicationConfiguration`

A `@Configuration` class with a single `@Bean` method that constructs `WorkflowService` with the injected `WorkflowRepository` (which Spring auto-wires to `JdbcWorkflowRepository` because it's annotated `@Repository`).

#### `application.yml`

Key configuration:
- **Datasource** — PostgreSQL URL, username, password, HikariCP pool size (all overridable via environment variables).
- **Flyway** — validates migration naming.
- **Jackson** — `default-property-inclusion: non_null` (omits null fields from JSON responses).
- **Server** — port 8080, graceful shutdown.
- **Actuator** — exposes `health`, `info`, `metrics` endpoints. Health probes enabled.

---

## 5. Database Schema

Managed by Flyway. Single migration: `V1__create_workflow_definition_schema.sql`.

### Entity-relationship overview

```
workflow_definition (1)
  ├──< workflow_version (N)
  │       ├──< workflow_task (N)
  │       └──< workflow_dependency (N)
```

### Table: `workflow_definition`

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `lifecycle_status` | VARCHAR(20) | `ACTIVE` or `ARCHIVED` (CHECK constraint) |
| `lock_version` | BIGINT | Optimistic lock counter, starts at 0 (CHECK ≥ 0) |
| `created_at` | TIMESTAMPTZ | |
| `updated_at` | TIMESTAMPTZ | |

### Table: `workflow_version`

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `workflow_id` | UUID FK → workflow_definition | |
| `version_number` | INTEGER | Starts at 1, monotonically increasing per workflow |
| `version_status` | VARCHAR(20) | `DRAFT` or `PUBLISHED` (CHECK constraint) |
| `name` | VARCHAR(200) | |
| `description` | VARCHAR(2000) | Nullable |
| `created_at` | TIMESTAMPTZ | |
| `updated_at` | TIMESTAMPTZ | |
| `published_at` | TIMESTAMPTZ | Nullable — must be NULL for DRAFT, non-NULL for PUBLISHED (CHECK constraint) |

**Key constraints:**
- `UNIQUE (workflow_id, version_number)` — no duplicate version numbers per workflow.
- **Partial unique index** `uq_workflow_single_draft` — `ON workflow_version(workflow_id) WHERE version_status = 'DRAFT'`. This is the database-level guarantee that **at most one DRAFT** exists per workflow at any time.
- Index `ix_workflow_version_lookup` on `(workflow_id, version_number DESC)` for fast "latest version" queries.

### Table: `workflow_task`

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `workflow_version_id` | UUID FK → workflow_version (ON DELETE CASCADE) | |
| `task_key` | VARCHAR(100) | e.g., `VALIDATE_ORDER` |
| `task_name` | VARCHAR(200) | Human-readable name |
| `task_type` | VARCHAR(100) | e.g., `NOOP`, `HTTP` |
| `configuration` | JSONB | Arbitrary config object (CHECK: must be a JSON object) |
| `position` | INTEGER | Ordering within the version (CHECK ≥ 0) |

**Key constraints:**
- `UNIQUE (workflow_version_id, task_key)` — no duplicate task keys within a version.
- `UNIQUE (workflow_version_id, position)` — no two tasks share the same position.

### Table: `workflow_dependency`

| Column | Type | Notes |
|---|---|---|
| `workflow_version_id` | UUID | Part of composite PK |
| `task_key` | VARCHAR(100) | The dependent task |
| `depends_on_task_key` | VARCHAR(100) | The prerequisite task |

**Key constraints:**
- Composite PK: `(workflow_version_id, task_key, depends_on_task_key)`.
- Two composite FKs referencing `workflow_task(workflow_version_id, task_key)` — both the dependent and the prerequisite must exist.
- `CHECK (task_key <> depends_on_task_key)` — no self-dependencies.
- `ON DELETE CASCADE` — when a task is deleted, its dependencies are automatically removed.
- Index on `(workflow_version_id, depends_on_task_key)` for efficient "what depends on X?" lookups.

---

## 6. What Happens When You Use It — End-to-End Flows

### 6.1 Create a workflow

```
Client                          Controller              Service            Repository              PostgreSQL
  │  POST /api/v1/workflows       │                       │                   │                       │
  │  { name, tasks, deps }        │                       │                   │                       │
  │──────────────────────────────►│                       │                   │                       │
  │                               │  Bean Validation      │                   │                       │
  │                               │  (WorkflowRequest)     │                   │                       │
  │                               │                       │                   │                       │
  │                               │  toDraft(request)     │                   │                       │
  │                               │  → WorkflowDraft      │                   │                       │
  │                               │  (DAG validation      │                   │                       │
  │                               │   runs in compact     │                   │                       │
  │                               │   constructor)        │                   │                       │
  │                               │──────────────────────►│                   │                       │
  │                               │                       │  create(draft)    │                       │
  │                               │                       │──────────────────►│                       │
  │                               │                       │                   │  BEGIN TRANSACTION    │
  │                               │                       │                   │  INSERT definition    │
  │                               │                       │                   │  INSERT version (DRAFT)│
  │                               │                       │                   │  INSERT tasks         │
  │                               │                       │                   │  INSERT dependencies  │
  │                               │                       │                   │  COMMIT               │
  │                               │                       │                   │◄──────────────────────│
  │                               │                       │  WorkflowDefinition│                     │
  │                               │                       │◄──────────────────│                       │
  │                               │  WorkflowResponse     │                   │                       │
  │  201 Created                  │                       │                   │                       │
  │  Location: /api/v1/workflows/{id}                     │                   │                       │
  │  ETag: "0"                    │                       │                   │                       │
  │  { id, versionStatus: DRAFT } │                       │                   │                       │
  │◄──────────────────────────────│                       │                   │                       │
```

**What happens:**
1. The request body is validated by Bean Validation annotations on `WorkflowRequest`.
2. The controller converts the request to a `WorkflowDraft` — at this point, the domain constructor runs full DAG validation (cycle detection, duplicate keys, dangling references, etc.). If invalid, `DomainValidationException` is thrown and mapped to `422 Unprocessable Content`.
3. The service delegates to the repository, which in a single transaction: inserts the definition (ACTIVE, lock_version=0), inserts version 1 (DRAFT), inserts all tasks and dependencies.
4. The response is `201 Created` with a `Location` header and an `ETag: "0"` (the initial lock version).

### 6.2 Read a workflow

```
GET /api/v1/workflows/{id}
```

1. Controller calls `service.get(id)`.
2. Service calls `repository.findById(id)`, which runs the `SELECT_CURRENT` query — a `LATERAL` join that picks the DRAFT if one exists, otherwise the latest PUBLISHED version.
3. Tasks and dependencies are loaded in separate queries.
4. Response is `200 OK` with an `ETag` header (the current `lock_version`).

### 6.3 List workflows

```
GET /api/v1/workflows?page=0&size=20
```

1. Controller calls `service.list(page, size)`.
2. Service validates `page ≥ 0` and `1 ≤ size ≤ 100`.
3. Repository counts all ACTIVE definitions, fetches a page of UUIDs (`LIMIT`/`OFFSET`), then loads each by ID.
4. Response is `200 OK` with `{ items, page, size, totalElements, totalPages }`.

### 6.4 Update a workflow

```
PUT /api/v1/workflows/{id}
If-Match: "0"
{ name, tasks, deps }
```

**What happens:**
1. The controller parses the `If-Match` header. If missing → `428 Precondition Required`. If malformed → `400 Bad Request`.
2. The request is validated and converted to a `WorkflowDraft` (DAG validation runs).
3. The service calls `repository.update(id, expectedLockVersion, draft)`.
4. Inside a transaction:
   - **`lockAndCheck`**: `SELECT ... FOR UPDATE` locks the `workflow_definition` row. Checks it exists, is ACTIVE, and `lock_version` matches. If not found → `WorkflowNotFoundException` (404). If version mismatch → `WorkflowConflictException` (409).
   - Check if a DRAFT version exists:
     - **Yes** → update its name/description and replace the task/dependency graph.
     - **No** (the draft was already published) → create a **new version** (version_number = max + 1) in DRAFT status, insert the graph.
   - Increment `lock_version` on the definition.
5. Response is `200 OK` with a new `ETag` (incremented lock version).

### 6.5 Publish a workflow

```
POST /api/v1/workflows/{id}/publish
If-Match: "1"
```

**What happens:**
1. Parse `If-Match`, validate request.
2. `lockAndCheck` — lock and verify version.
3. `UPDATE workflow_version SET version_status = 'PUBLISHED', published_at = NOW() WHERE workflow_id = :id AND version_status = 'DRAFT'`.
4. If 0 rows changed (no draft to publish) → `WorkflowConflictException` (409).
5. Increment `lock_version`.
6. Response is `200 OK` with `versionStatus: PUBLISHED`, `publishedAt` set, and a new ETag.

**Key invariant:** Once published, a version is **never mutated**. The task graph, name, and description are frozen. To change the workflow, you call `PUT` (update), which creates a *new* DRAFT version.

### 6.6 Archive a workflow

```
DELETE /api/v1/workflows/{id}
If-Match: "2"
```

**What happens:**
1. Parse `If-Match`, validate.
2. `lockAndCheck` — lock and verify version.
3. `UPDATE workflow_definition SET lifecycle_status = 'ARCHIVED', lock_version = lock_version + 1`.
4. Response is `204 No Content`.

**Key invariant:** Archiving is a **soft delete**. All version history, tasks, and dependencies remain in the database. The workflow simply no longer appears in `GET` (single) or `GET` (list) responses, because those queries filter on `lifecycle_status = 'ACTIVE'`.

---

## 7. Concurrency Model

FlowForge uses **optimistic locking** backed by a database-enforced version counter.

### How it works

1. Every `workflow_definition` row has a `lock_version` column (BIGINT, starts at 0).
2. Every read response includes an `ETag` header: `ETag: "0"` (the lock version in quotes).
3. Every mutation (update, publish, archive) requires the client to send `If-Match: "0"` — the ETag they received.
4. The server:
   - Acquires a **pessimistic row lock** (`SELECT ... FOR UPDATE`) on the definition row. This prevents two concurrent transactions from both passing the version check.
   - Compares the `lock_version` in the database with the `If-Match` value.
   - If they match → proceed, then increment `lock_version`.
   - If they don't match → `409 Conflict` ("Workflow was modified concurrently; expected version X but found Y").
5. If `If-Match` is missing entirely → `428 Precondition Required`.

### Why both pessimistic lock AND optimistic version check?

The `SELECT ... FOR UPDATE` ensures that two concurrent transactions **serialize** — the second one waits for the first to commit. Then the version check catches the case where the data changed between the client's read and their write. This belt-and-suspenders approach is deliberate: the pessimistic lock prevents race conditions within the transaction, and the version check prevents stale writes from clients with outdated ETags.

### ETag format

ETags are **strong** and **numeric**: `"0"`, `"1"`, `"2"`, etc. The controller validates the format with a regex: `"[0-9]+"`. This is simpler and more deterministic than hash-based ETags, and it directly corresponds to the `lock_version` column.

---

## 8. Version Lifecycle

### Workflow lifecycle

```
                    POST /api/v1/workflows
                    ┌─────────────┐
                    │   ACTIVE     │
                    │ (lock_ver=0) │
                    └──────┬───────┘
                           │
                    DELETE /{id} (If-Match)
                           │
                           ▼
                    ┌─────────────┐
                    │   ARCHIVED    │
                    │ (soft delete) │
                    └─────────────┘
```

Once archived, a workflow is invisible to all read operations but its data is preserved.

### Version lifecycle

```
POST /{id} (create)
    │
    ▼
┌─────────┐  POST /{id}/publish  ┌───────────┐
│  DRAFT   │ ──────────────────► │ PUBLISHED  │
│ version 1│                      │ (immutable)│
└─────────┘                      └─────┬─────┘
                                       │
                                 PUT /{id} (update)
                                       │
                                       ▼
                                 ┌─────────┐
                                 │  DRAFT   │
                                 │ version 2│
                                 └─────────┘
```

**Rules:**
- At most **one DRAFT** per workflow at any time (enforced by a partial unique index).
- A PUBLISHED version is **never modified** — its name, description, tasks, and dependencies are frozen.
- Publishing a DRAFT flips it to PUBLISHED and sets `published_at`.
- Updating after publication creates a **new DRAFT** with the next version number.
- The "current" version (returned by `GET /{id}`) is: the DRAFT if one exists, otherwise the latest PUBLISHED.

### The "current version" query

```sql
SELECT w.*, v.*
  FROM workflow_definition w
  JOIN LATERAL (
    SELECT candidate.*
      FROM workflow_version candidate
     WHERE candidate.workflow_id = w.id
     ORDER BY CASE WHEN candidate.version_status = 'DRAFT' THEN 0 ELSE 1 END,
              candidate.version_number DESC
     LIMIT 1
  ) v ON TRUE
 WHERE w.id = :id
   AND w.lifecycle_status = 'ACTIVE'
```

The `LATERAL` subquery picks exactly one version: DRAFTs sort first (priority 0), then PUBLISHED by descending version number. `LIMIT 1` ensures a single result.

---

## 9. Error Handling

All errors are returned as **RFC 9457 Problem Details** (`application/problem+json`):

```json
{
  "type": "about:blank",
  "title": "Unprocessable Content",
  "status": 422,
  "detail": "Workflow definition is invalid",
  "instance": "/api/v1/workflows",
  "code": "INVALID_WORKFLOW",
  "violations": [
    "workflow graph must be acyclic",
    "duplicate task key: A"
  ]
}
```

### Error catalog

| Scenario | HTTP Status | `code` | When |
|---|---|---|---|
| Workflow not found or archived | 404 | `WORKFLOW_NOT_FOUND` | `GET`/`PUT`/`POST`/`DELETE` on a non-existent or archived workflow |
| Stale ETag (concurrent modification) | 409 | `WORKFLOW_CONFLICT` | `lock_version` in `If-Match` doesn't match the database |
| Missing `If-Match` header | 428 | `IF_MATCH_REQUIRED` | Mutation without `If-Match` |
| Domain validation failure (cycles, duplicates, etc.) | 422 | `INVALID_WORKFLOW` | `WorkflowDraft` construction fails; includes `violations` array |
| Bean Validation failure (blank fields, bad patterns) | 400 | `INVALID_REQUEST` | Request DTO fails `@Valid`; includes `violations` array |
| Malformed `If-Match` or bad pagination params | 400 | `INVALID_REQUEST` | `IllegalArgumentException` |
| No draft to publish | 409 | `WORKFLOW_CONFLICT` | `POST /{id}/publish` when no DRAFT exists |

---

## 10. Testing Strategy

FlowForge has four layers of tests, each targeting a different concern:

### 10.1 Domain unit tests (`WorkflowDraftTest`)

**Location:** `flowforge-domain/src/test/java/`

Tests the `WorkflowDraft` record's validation logic in isolation:
- `acceptsAValidDag` — a linear chain (VALIDATE → PAY → NOTIFY) constructs successfully.
- `rejectsCycles` — a circular dependency (A → B → C → A) throws `DomainValidationException` with the message "workflow graph must be acyclic".
- `rejectsDuplicateKeysAndUnknownDependencies` — duplicate task keys and a dependency referencing a non-existent task both fail.

These tests run without any framework — just JUnit 5 and AssertJ.

### 10.2 HTTP/controller tests (`WorkflowControllerTest`)

**Location:** `flowforge-control-plane/src/test/java/.../adapter/in/web/`

Uses `MockMvc` with a **mocked** `WorkflowRepository` (no database). Tests:
- `createsAWorkflowAndReturnsLocationAndEtag` — POST returns 201, `Location` header, and `ETag: "0"`.
- `requiresIfMatchForMutation` — PUT without `If-Match` returns 428.
- `reportsCyclesAsUnprocessableContent` — a cyclic graph returns 422 with `INVALID_WORKFLOW` code and the violation message.

### 10.3 Architecture test (`ArchitectureTest`)

**Location:** `flowforge-control-plane/src/test/java/`

Uses **ArchUnit** to enforce the dependency rule:
- No class in `io.flowforge.domain..` or `io.flowforge.application..` may depend on `io.flowforge.controlplane..`, `org.springframework..`, or `jakarta..`.

This test is the **executable guardrail** for the clean architecture. If someone accidentally imports Spring in the domain module, the build fails.

### 10.4 Persistence integration tests (`WorkflowPersistenceIntegrationTest`)

**Location:** `flowforge-control-plane/src/test/java/`

Uses **Testcontainers** to spin up a real PostgreSQL 17.6 container. Annotated with `@Testcontainers(disabledWithoutDocker = true)` — automatically skipped when Docker isn't available. Tests:
- `persistsPublishesAndCreatesANewDraftWithoutMutatingPublishedVersion` — full lifecycle: create draft → publish → update (creates new draft v2) → verify list count.
- `rejectsAStaleWriterAndArchivesWithoutDeletingHistory` — stale ETag (version 99) throws `WorkflowConflictException`; archive makes the workflow invisible to `get`.

---

## 11. How to Run

### Prerequisites

- **Java 21**
- **Docker** (for PostgreSQL and integration tests)

### Start PostgreSQL

```powershell
docker compose up -d postgres
```

### Run the control plane

```powershell
.\mvnw.cmd -pl flowforge-control-plane -am spring-boot:run
```

The API will be available at `http://localhost:8080/api/v1/workflows`.

### Run all tests

```powershell
.\mvnw.cmd verify
```

> Tests requiring PostgreSQL use Testcontainers and are skipped when Docker is not available. All other tests still run.

### Configuration

| Environment variable | Default |
|---|---|
| `FLOWFORGE_DB_URL` | `jdbc:postgresql://localhost:5432/flowforge` |
| `FLOWFORGE_DB_USERNAME` | `flowforge` |
| `FLOWFORGE_DB_PASSWORD` | `flowforge` |
| `FLOWFORGE_DB_POOL_SIZE` | `10` |
| `PORT` | `8080` |

### Quick API example

```bash
# Create a workflow
curl -X POST http://localhost:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Order processing",
    "description": "Validate, charge, and notify",
    "tasks": [
      {"key": "VALIDATE_ORDER", "name": "Validate order", "type": "NOOP", "configuration": {}},
      {"key": "PROCESS_PAYMENT", "name": "Process payment", "type": "HTTP", "configuration": {"uri": "/payments"}}
    ],
    "dependencies": [
      {"taskKey": "PROCESS_PAYMENT", "dependsOnTaskKey": "VALIDATE_ORDER"}
    ]
  }'

# Response includes ETag header, e.g.: ETag: "0"
# and Location: /api/v1/workflows/{id}

# Publish it (use the ETag from the create response)
curl -X POST http://localhost:8080/api/v1/workflows/{id}/publish \
  -H 'If-Match: "0"'

# Update it (creates a new draft, use the ETag from publish)
curl -X PUT http://localhost:8080/api/v1/workflows/{id} \
  -H "Content-Type: application/json" \
  -H 'If-Match: "1"' \
  -d '{ "name": "Order processing v2", "tasks": [{"key": "VALIDATE_ORDER", "name": "Validate order", "type": "NOOP", "configuration": {}}] }'
```

---

## 12. Roadmap — Where This Is Going

FlowForge is designed to grow into a full distributed orchestration platform. The phases (from `docs/ROADMAP.md`):

| Phase | Status | Scope |
|---|---|---|
| **Phase 1** | ✅ Completed | Project foundation and versioned workflow definitions |
| **Phase 2** | 🔜 Planned | Workflow state machine, execution, and DAG resolution |
| **Phase 3** | 🔜 Planned | Kafka messaging, transactional outbox, and distributed workers |
| **Phase 4** | 🔜 Planned | Retries, timeouts, dead-letter queues, and failure recovery |
| **Phase 5** | 🔜 Planned | Durable scheduling, Redis coordination, and backpressure |
| **Phase 6** | 🔜 Planned | Observability, scalability validation, and resilience testing |
| **Phase 7** | 🔜 Planned | Security, delivery automation, and operational readiness |

The clean architecture established in Phase 1 ensures that execution (Phase 2), messaging (Phase 3), and reliability (Phase 4) can be added **without changing the definition model or the API contract**. The domain invariants, version lifecycle, and concurrency model established here are the foundation everything else builds on.

---

*See also: [README.md](README.md) for quick-start, [ROADMAP.md](docs/ROADMAP.md) for phase status, and [ADR-001](docs/adr/001-phase-1-architecture.md) for the major design decisions.*
