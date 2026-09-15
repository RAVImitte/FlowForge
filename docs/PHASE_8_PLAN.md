# Phase 8 - Performance Hardening and Capacity Validation

Status: **IN PROGRESS**

Branch: `feature/phase-8-performance-hardening`

## Objective

Raise FlowForge's verified single-machine capacity without weakening its delivery, fencing, idempotency, tenant-isolation, or recovery guarantees. Targets in this document are acceptance criteria, not achieved benchmark claims.

The primary acceptance workload is 10,000 workflows with ten tasks each over 30 minutes. That requires at least 55.6 completed tasks/second; 100 tasks/second is the stretch target.

## Guardrails

- PostgreSQL remains the durable correctness boundary. Redis remains an optional coordination accelerator and reconstructable mirror.
- Kafka delivery remains at-least-once, with logical effects protected by inbox deduplication, idempotency keys, and fencing tokens.
- Capacity must not be obtained by hiding failures, increasing lease durations without evidence, or relaxing tenant and concurrency limits.
- Overload must fail explicitly with HTTP 429 and `Retry-After` before database exhaustion creates ambiguous HTTP 500 outcomes.
- Every published result must identify workload shape, topology, warm-up, duration, host resources, and reconciliation evidence.
- A target is complete only after repeatable runs; aspirational values must stay labelled as targets.

## Acceptance targets

| Dimension | Required target | Stretch target |
|---|---:|---:|
| Sustained completion throughput | 55.6 tasks/sec for 30 minutes | 100 tasks/sec for 30 minutes |
| Dataset | 10,000 workflows / 100,000 task executions | Same |
| Concurrent executing workflows | 100 | 250 |
| Worker topology | 4 workers / 12 Kafka partitions | Same |
| Logical correctness | 100% reconciled; zero lost or duplicate logical completions | Same |
| Expected overload response | HTTP 429 with `Retry-After`; zero unexpected 5xx | Same |
| Worker distribution | No worker more than 15% from the mean | No worker more than 10% from the mean |
| Workflow-start API latency | p95 at or below 150 ms at accepted target load | p95 below 100 ms |
| Ten-task completion latency | p95 at or below 5 seconds | p95 below 3 seconds |

## Execution plan

### Slice 8.1 - Benchmark contract and admission protection (in progress)

- Add a fail-fast, per-control-plane workflow-start bulkhead ahead of PostgreSQL.
- Retain PostgreSQL-backed tenant, ready-queue, and workflow-concurrency limits as the distributed correctness layer.
- Meter admitted in-flight starts and rejected starts.
- Extend API tests for stable HTTP 429 problem details and `Retry-After`.
- Extend the load harness to classify 202, expected 429, and unexpected responses separately.
- Re-run the prior 100-workflow pressure case and require zero unexpected 5xx before advancing.

### Slice 8.2 - Lease and result-ingestion isolation

- Measure executor, consumer, connection-pool, lock, and Kafka-lag contention during pressure runs.
- Reserve processing capacity for worker heartbeats and task results so workflow starts and outbox publication cannot starve recovery-critical traffic.
- Remove `WORKER_LEASE_EXPIRED` failures at the 100-workflow pressure boundary without blind timeout inflation.
- Require exact durable state reconciliation after every run.

### Slice 8.3 - PostgreSQL and Kafka hot-path optimization

- Capture query plans and transaction timings for workflow admission, ready-task claims, result ingestion, and outbox/inbox operations.
- Shorten workflow-root lock scope and add or adjust indexes only from measured evidence.
- Batch safe outbox/inbox work and tune consumer concurrency, JDBC pool allocation, and polling bounds.
- Establish a repeatable 50 tasks/sec intermediate gate before scaling the topology.

### Slice 8.4 - Four-worker, twelve-partition scaling

- Provide a reproducible four-worker / twelve-partition local topology.
- Verify cooperative rebalancing, partition ownership, fencing, and bounded shutdown during scale-out and replacement.
- Demonstrate 100-250 concurrently executing workflows and worker imbalance within the acceptance bound.

### Slice 8.5 - Thirty-minute capacity and scheduler soak

- Complete 10,000 ten-task workflows and 100,000 task executions over 30 minutes.
- Record API and completion percentiles, Kafka lag, database connections and locks, JVM CPU/heap/GC, retries, lease recovery, and worker distribution.
- Repeat the workload through durable scheduling with no missed or duplicate schedule fires.
- Preserve raw evidence and generate a machine-readable benchmark summary.

### Slice 8.6 - Failure validation and closeout

- Inject worker termination, duplicate Kafka delivery, Kafka interruption, Redis loss, and control-plane replacement under meaningful load.
- Prove recovery, fencing, replay safety, and exact durable reconciliation.
- Run the complete test, security, upgrade, recovery, manifest, and SBOM gates.
- Update the capacity model, runbooks, ADR, roadmap, and resume evidence using measured results only.

## Current measured baseline

- Safe soak: 120 workflows / 1,200 tasks at 0.501 workflows/sec, 117 ms workflow-start p95, and 2.481-second completion p95.
- Two-worker pressure run: 82 of 100 workflows completed at 0.727 workflows/sec; work split 77/73 with no duplicate logical processing in the earlier distribution test.
- Overload probe: 1,000 starts offered at 100/sec produced 930 HTTP 500 responses and no HTTP 429 responses, with database connection and lock pressure.

These figures define the starting boundary. They are not evidence that the Phase 8 targets have been achieved.

## Completion checklist

- [ ] Slice 8.1 benchmark contract and admission protection
- [ ] Slice 8.2 lease and result-ingestion isolation
- [ ] Slice 8.3 PostgreSQL and Kafka hot-path optimization
- [ ] Slice 8.4 four-worker / twelve-partition scaling
- [ ] Slice 8.5 thirty-minute capacity and scheduler soak
- [ ] Slice 8.6 failure validation and closeout

