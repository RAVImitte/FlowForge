# ADR-002: Durable Phase 2 execution architecture

## Status

Accepted

## Context

Phase 2 must execute immutable workflow DAGs correctly before Kafka, Redis, retries, or distributed workers are introduced. Execution state must survive restarts, concurrent task completions must not release a dependent task twice, and API retries must not create duplicate workflow runs.

## Decision

PostgreSQL is the durable coordination boundary for Phase 2.

- Every execution references the exact published `workflow_version` used to materialize it.
- Workflow and task states are immutable domain values with explicit transition matrices and monotonic `state_version` fields.
- Starting a workflow requires an `Idempotency-Key`, enforced by a unique `(workflow_id, idempotency_key)` constraint.
- Root tasks materialize as `READY`; all other tasks materialize as `BLOCKED`.
- Ready tasks are claimed in bounded batches using `FOR UPDATE SKIP LOCKED`.
- Completion and cancellation lock the workflow row before task rows. This common lock order serializes DAG advancement and avoids duplicate fan-in release.
- A blocked task becomes ready only when every prerequisite has status `SUCCEEDED`.
- Each claim creates a durable task attempt; every transition appends an ordered execution event.
- Duplicate completion with the same terminal result is a no-op. A conflicting terminal result is rejected.
- A scheduled in-process dispatcher handles `NOOP`, `DELAY`, and `FAIL` tasks behind an application port. It also claims durable ready work at startup.

## Failure semantics

- A task failure, timeout, or unexpected cancellation fails the workflow and cancels work that has not started.
- Already-running sibling tasks may finish and record their outcomes, but they cannot change a terminal workflow result.
- Workflow cancellation first enters `CANCELLING`, prevents dependent work from being released, cancels queued tasks, and becomes `CANCELLED` after running tasks finish.
- Recovery of abandoned running tasks, retry scheduling, worker leases, and fencing are deferred to Phase 4 because they require retry and distributed-worker semantics.

## Consequences

The Phase 2 execution path is durable, observable, horizontally claimable, and testable without coupling the domain or application modules to Spring. PostgreSQL locking introduces short serialization points per workflow, which is intentional: different workflows execute concurrently, while transitions within one workflow remain deterministic. Kafka can replace the in-process dispatcher in Phase 3 without changing the execution state machine or repository contract.
