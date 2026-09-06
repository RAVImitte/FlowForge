# Phase 6 resilience evidence

Slices 6.6a and 6.6b validate bounded PostgreSQL, Kafka, Redis, and telemetry-backend failures against the single-control-plane, single-worker, six-partition topology. These are local Rancher Desktop measurements for the checked-in workload, not universal production recovery guarantees.

| Dependency | Injected pause | Workflows | Completion p95 | Recovery upper bound | Suppressed redeliveries | Durable failures |
|---|---:|---:|---:|---:|---:|---:|
| PostgreSQL | 5 s | 10/10 | 37.212 s | 21 s | 0 | 0 |
| Kafka | 5 s | 10/10 | 42.109 s | 25 s | 8 | 0 |
| Redis pause + flush | 5 s | 10/10 | 41.227 s | 5 s | 0 | 0 |
| Prometheus + collector | 5 s | 10/10 | 28.601 s | 7 s | 0 | 0 |

`recoverySeconds` is a conservative workload-level bound measured from the latest dependency restoration timestamp until every load operation finishes. It is not a low-level socket-reconnect measurement.

The Kafka scenario intentionally permits a bounded number of suppressed command redeliveries. Eight duplicate deliveries demonstrate the expected at-least-once acknowledgement uncertainty; durable inbox handling suppressed them while all ten workflow executions converged to `SUCCEEDED` exactly once. One Kafka lag probe degraded to explicit error evidence during the outage without terminating workload execution.

The Redis scenario applied a five-second outage and then `FLUSHALL` while PostgreSQL held active workflow/task permits. Redis recorded 15 degraded coordination operations, the reconciliation loop ran 334 times, ten namespace keys were visible immediately after reconstruction, and sampled PostgreSQL state showed zero permit-limit violations. PostgreSQL remained the authoritative fencing boundary throughout.

The telemetry scenario paused Prometheus and the OpenTelemetry Collector together. Workflow delivery remained 10/10 with zero durable failures; after restoration Prometheus reported both application targets healthy and the collector logged 11 exported trace batches. Telemetry recovery was therefore observed independently from workflow correctness.

Run the complete required matrix with isolated state and automatic report curation:

```powershell
.\scripts\run-resilience-matrix.ps1
```

Reproduce the scenarios with:

```powershell
.\scripts\run-load-topology.ps1 -Topology single-6p -Profile resilience -FaultPlan postgres-pause
.\scripts\run-load-topology.ps1 -Topology single-6p -Profile resilience -FaultPlan kafka-pause -SkipBuild
.\scripts\run-load-topology.ps1 -Topology single-6p -Profile resilience-coordination -FaultPlan redis-loss -SkipBuild
.\scripts\run-load-topology.ps1 -Topology single-6p -Profile resilience -FaultPlan telemetry-pause -SkipBuild
```

The runner limits each configured delay and outage to 30 seconds, records exact fault/restoration timestamps, requires workload overlap, and defensively unpauses targeted dependencies during cleanup. Redis flush is restricted to Redis and proves namespace reconstruction; telemetry runs enable bounded tracing only for the scenario and verify backend recovery. All four closeout scenarios are required and passed. The curated aggregate is in `resilience-summary.json`; complete redacted reports are under `reports/`.
