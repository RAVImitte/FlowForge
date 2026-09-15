# FlowForge load-testing harness

This harness produces bounded, repeatable fan-out/fan-in workflow traffic through the public HTTP API. It uses paced arrivals, Java 21 virtual threads, a hard in-flight limit, unique idempotency keys, terminal polling, nearest-rank p50/p95/p99 latency, machine-readable JSON output, and executable thresholds.

Profiles:

- `smoke`: short correctness and environment check.
- `overload`: bounded high arrival rate that verifies rejection/error limits and recovery.
- `soak`: longer steady-state run intended to expose queue or resource growth.
- `scheduled`: creates one-time schedules and measures observed fire lag from the requested instant until durable schedule completion.
- `resilience`: retry-capable bounded dependency-failure traffic.
- `resilience-coordination`: a longer coordinated workload that keeps PostgreSQL permits active through Redis pause and state reconstruction.

To target an already running distributed topology, use:

```powershell
.\scripts\run-load-test.ps1 -Profile smoke
```

Reports are written under `load-testing/results/` and ignored by default so accidental laptop results are not presented as universal capacity evidence. Curated reports for the measured topology matrix will be copied to `docs/performance/` during Slice 6.5c with machine, JVM, replica, partition, pool, and dependency metadata.

Exit code `0` means every configured threshold passed. Exit code `2` from the executable means the run completed but at least one threshold failed. The PowerShell wrapper converts any non-zero code into a terminating error suitable for CI or scripted matrix execution. Report schema V3 separates admission HTTP statuses from polling traffic, fails every profile on an unexpected admission 5xx, and distinguishes terminal operation transport errors from transient polling transport errors retried for accepted executions.

The API profile's completion latency begins after the start request is accepted and ends when the execution reaches a terminal state. The scheduled profile's completion latency is schedule fire lag, not workflow terminal latency; the topology probe added in Slice 6.5b will capture terminal workflow counts independently.

## Managed topology runs

The topology runner builds the executable artifacts, starts PostgreSQL/Kafka/Redis, launches named application replicas on explicit ports, verifies health and Kafka topic partition counts, runs the workload, writes one combined report, then stops every process and container it started:

```powershell
.\scripts\run-load-topology.ps1 -Topology single-6p -Profile smoke
.\scripts\run-load-topology.ps1 -Topology workers-2-6p -Profile smoke -SkipBuild
.\scripts\run-load-topology.ps1 -Topology balanced-2x2-6p -Profile smoke -SkipBuild
.\scripts\run-load-topology.ps1 -Topology workers-4-c3-12p -Profile phase8-intermediate -SkipBuild
```

Named topologies live in `load-testing/topologies/` and declare control-plane/worker ports, heap bounds, database-pool sizes, worker concurrency, Kafka partitions, and probe timing. Multi-control-plane runs distribute API operations round-robin; Kafka consumer groups independently distribute command, result, and heartbeat partitions.

Phase 8 adds a four-worker/twelve-partition topology, a bounded 1,000-workflow intermediate gate, and explicit 10,000-workflow API and scheduler soak profiles. The long profiles are acceptance workloads and must not be described as achieved until their generated reports pass exact durable reconciliation.

Each ignored run directory contains the derived profile, workload report, combined `report.json`, per-process logs, and the load-generator exit code. The combined report records host/JVM/Docker/Git metadata; per-replica CPU, heap, throughput, retry, timeout, and duplicate counters; queue depth/age; PostgreSQL connections and blocked locks; Kafka lag and partition ownership; durable execution counts; and useful-work distribution. A run passes only when workload thresholds, process exit, and durable database reconciliation all pass.

`-ResetData` is intentionally opt-in because it executes `docker compose down --volumes --remove-orphans` before the run. Use it only when a clean data set or a lower Kafka partition count is required. `-KeepInfrastructure` leaves PostgreSQL, Kafka, and Redis running, but application and load-generator processes are still stopped deterministically.

## Bounded dependency faults

Versioned fault plans in `load-testing/faults/` can pause PostgreSQL, Kafka, Redis, Prometheus, or the OpenTelemetry Collector only after the workload process starts. Redis additionally supports a scoped local `FLUSHALL` test. Every action is limited to a 30-second delay and 30-second outage. The injector records exact fault and restoration timestamps, and the topology runner defensively restores every targeted service and cleans up owned processes even when a threshold or probe fails.

```powershell
.\scripts\run-load-topology.ps1 -Topology single-6p -Profile resilience -FaultPlan postgres-pause
.\scripts\run-load-topology.ps1 -Topology single-6p -Profile resilience -FaultPlan kafka-pause -SkipBuild
.\scripts\run-load-topology.ps1 -Topology single-6p -Profile resilience-coordination -FaultPlan redis-loss -SkipBuild
.\scripts\run-load-topology.ps1 -Topology single-6p -Profile resilience -FaultPlan telemetry-pause -SkipBuild
.\scripts\run-resilience-matrix.ps1 -SkipBuild
```

Fault-aware report schema V2 requires the fault window to overlap the workload and evaluates bounded recovery time, suppressed at-least-once command redeliveries, durable failures, ordinary workload thresholds, and durable reconciliation together. Redis plans additionally assert degraded-operation evidence, reconciliation, mirror reconstruction, and zero PostgreSQL permit violations. Telemetry plans require recovered Prometheus targets, collector connectivity, and post-restoration trace export. A non-zero suppressed-duplicate count is expected evidence under acknowledgement uncertainty; it passes only within the plan's explicit bound and when durable reconciliation remains exact.
