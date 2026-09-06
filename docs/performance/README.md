# FlowForge local capacity evidence

This directory contains curated, machine-readable measurements for the Phase 6 capacity matrix. The results describe one Windows host running Rancher Desktop and are not universal production capacity claims.

## Method

Every scenario provisions the same real PostgreSQL, Kafka, and Redis dependencies, starts independently scalable control-plane and worker JVMs, drives fan-out/fan-in workflows through the public API, and reconciles client observations with durable PostgreSQL state. Each report includes workload latency and throughput, per-replica useful work, JVM resources, database connections and blocked locks, Kafka lag and partition ownership, queue age, retries, timeouts, duplicates, and environment metadata.

The versioned scenario catalog is `load-testing/capacity-matrix.json`. Run all scenarios from the repository root:

```powershell
.\scripts\run-capacity-matrix.ps1
```

Run only selected scenarios while iterating:

```powershell
.\scripts\run-capacity-matrix.ps1 -Scenario baseline,horizontal-workers -SkipBuild
```

Raw logs and reports stay under ignored `load-testing/results/`. Curated reports have the local machine name redacted and live under `docs/performance/reports/`.

## Interpretation rules

- Compare scenarios only when their workload profile is identical.
- Treat Kafka partitions as an upper bound on simultaneously active consumers for a consumer group, not a throughput guarantee.
- Treat database connections, queue age, and consumer lag as saturation evidence; replica count alone is not evidence of increased capacity.
- A successful laptop run establishes correctness and a local operating point, not a production service-level commitment.
- Tune only one constrained resource at a time, rerun the same workload, and preserve the before/after reports.

The completed measurements, safe envelope, bottlenecks, scaling limits, overload behavior, and tuning order are documented in [CAPACITY_MODEL.md](CAPACITY_MODEL.md). The aggregate machine-readable result is [capacity-matrix-summary.json](capacity-matrix-summary.json).
