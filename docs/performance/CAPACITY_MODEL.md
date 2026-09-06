# FlowForge local capacity model

This model is derived from the isolated matrix in `capacity-matrix-summary.json`. It describes one Windows 11 host with 16 logical processors and 16 GB RAM running Java 21 and Rancher Desktop. It is evidence for architecture and tuning decisions, not a production capacity commitment.

## Measured operating points

Each scenario starts with fresh PostgreSQL and Kafka volumes. The control plane provisions six-partition topics and reaches readiness before workers start. Percentiles are client-observed; workflow rates count successful terminal workflows per second. Scheduled completion p95 is trigger fire lag.

| Scenario | Offered workload | Result | Start p95 | Completion/fire-lag p95 | Successful workflow rate |
|---|---:|---:|---:|---:|---:|
| Single-instance smoke | 25 workflows, 5/s, 6 tasks each | 25/25 | 425 ms | 11.575 s | 1.116/s |
| Balanced 2x2 smoke | 25 workflows, 5/s, 6 tasks each | 25/25 | 767 ms | 15.050 s | 1.038/s |
| Scheduled safe | 25 schedules, 1/s | 25/25 | 153 ms | 1.840 s | 0.895/s |
| Two-worker soak | 120 workflows, 0.5/s, 10 tasks each | 120/120 | 117 ms | 2.481 s | 0.501/s |
| Single capacity pressure | 100 workflows, 10/s, 10 tasks each | 47/100 | 1.929 s | 56.522 s | 0.455/s |
| Two-worker capacity pressure | same workload | 82/100 | 1.352 s | 64.464 s | 0.727/s |
| Balanced 2x2 capacity pressure | same workload | 64/100 | 6.316 s | 79.153 s | 0.540/s |
| Pool size 2 pressure | same workload | 20/100 | 2.122 s | 57.531 s | 0.178/s |
| Eight consumers / six partitions | same workload | 63/100 | 3.340 s | 53.513 s | 0.563/s |

The verified local safe envelope is therefore the checked-in smoke, scheduled-safe, and soak profiles. The matrix does not claim that unmeasured points between them pass.

## Scaling limits and first bottleneck

Worker command parallelism is bounded by:

`active command consumers <= min(Kafka partitions, worker replicas * listener concurrency)`

The partition-bound run configured eight consumers but observed six active consumers and six assignments. Extra consumers could not add Kafka parallelism and produced a 50% useful-work imbalance between worker replicas.

At the common 100-workflow pressure point, a second worker improved successful completions from 47 to 82 and throughput from 0.455/s to 0.727/s. It did not make the point safe: result lag peaked at 208 and 18 attempts lost their worker lease. Adding a second control plane reduced the outcome to 64 successes while blocked locks peaked at 13 and start p95 rose to 6.316 seconds. Control-plane replicas increase availability, but shared-row contention and result-ingestion capacity must be tuned before they increase throughput.

The first repeatable failure mechanism was `WORKER_LEASE_EXPIRED`, caused by delayed result/heartbeat processing under pressure. A two-connection pool made the constraint explicit: outbox age reached 10.35 seconds, result lag reached 281, and only 20 workflows succeeded. The sustained soak stayed below this boundary: all 120 workflows and 1,200 tasks succeeded, work split 599/601 across workers, and result lag peaked at four.

## Overload and scheduling behavior

At 1,000 starts offered at 100/s, 95 returned HTTP 202, 905 start calls received an unexpected response, and the HTTP status sample contained 930 total 500 responses. PostgreSQL connections reached 41 and blocked locks reached 18. Durable state contained 142 failed workflows—more than the 95 acknowledged starts—demonstrating uncertain HTTP outcomes: clients must reuse idempotency keys after a 5xx and query execution state instead of assuming the transaction was absent. No 429 responses were observed, so workflow-start admission shedding must be added before treating this as a production overload posture.

The 100-schedule burst completed all schedule records but missed its five-second fire-lag objective with 11.464-second p95; at the report snapshot 37 workflows had succeeded and 63 were still running. The one-per-second scheduled-safe profile completed all workflows with 1.840-second fire-lag p95.

## Tuning sequence

1. Define the workflow shape, arrival rate, completion objective, and acceptable queue age.
2. Keep topic provisioning ahead of worker startup and size Kafka partitions for the desired active consumer count.
3. Scale worker replicas/listener concurrency only up to the partition bound, then verify useful-work balance.
4. Size worker and control-plane PostgreSQL pools for handlers, result ingestion, lease renewal, and recovery loops; watch pool waits, locks, and outbox age together.
5. Tune control-plane result-consumer concurrency and reduce shared-row transaction contention before adding control-plane replicas for throughput.
6. Keep heartbeat execution isolated, and maintain task leases comfortably above observed heartbeat/result-processing delay.
7. Add workflow-start admission limits that shed with 429 before database pool exhaustion produces uncertain 5xx outcomes.
8. Rerun the identical isolated matrix on production-like hardware; do not extrapolate these laptop numbers.

The raw run is ignored under `load-testing/results/capacity-matrix-20260906-115555`; curated reports in `docs/performance/reports/` retain workload, topology, distribution, lag, resource, correctness, and durable failure evidence with the machine name redacted.
