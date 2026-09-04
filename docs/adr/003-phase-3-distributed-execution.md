# ADR-003: Kafka-based distributed execution

## Status

Accepted

## Context

Phase 2 deliberately kept execution in the control-plane process. Phase 3 must move task handling to independently scalable workers while preserving PostgreSQL workflow-state correctness across broker failures, duplicate delivery, concurrent instances, and process restarts.

Kafka delivery is at least once. A producer can publish successfully and fail before recording that acknowledgement, and a consumer can finish its database transaction before recording its Kafka offset. FlowForge therefore cannot honestly claim end-to-end exactly-once execution.

## Decision

PostgreSQL remains the system of record and Kafka is the durable transport between the control plane and workers.

- The control plane commits workflow transitions and outbound commands/events to one PostgreSQL transaction.
- Control-plane publishers claim outbox rows with `FOR UPDATE SKIP LOCKED`, a bounded lease, and a fencing token.
- Workers commit the command inbox, handler outcome, and result outbox atomically before acknowledging the command.
- The result consumer commits its inbox row, task transition, DAG advancement, lifecycle events, and downstream commands atomically before acknowledging the result.
- Stable event IDs and unique inbox keys make broker redelivery harmless.
- Conflicting reuse of an event ID or stale task state version is rejected instead of silently accepted.
- The `production` Spring profile selects the complete Kafka topology. The in-process Phase 2 path remains the default development fallback.
- Startup validation rejects competing dispatchers, Kafka components without Kafka, and incomplete distributed execution topology.

The control-plane and worker migrations use independent Flyway history tables. Baseline version `0` allows either service to initialize first when they share one PostgreSQL schema while still applying both services' V1 migrations. Separate worker database credentials remain supported.

## Topics and partition keys

| Topic | Record key | Reason |
|---|---|---|
| `flowforge.task.commands.v1` | task execution ID | Keeps redeliveries for one task ordered while distributing unrelated tasks across workers. |
| `flowforge.task.results.v1` | workflow execution ID | Serializes results for one workflow partition while allowing unrelated workflows to advance concurrently. |
| `flowforge.execution.events.v1` | workflow execution ID | Preserves the observable lifecycle order for one workflow. |

Topics are explicitly versioned. Partition count and replication factor are deployment configuration, not contract fields. Consumer-group capacity is bounded by topic partitions: adding instances beyond the assigned partition count improves failover but not active consumption throughput. Listener concurrency must be sized together with instance count and database capacity.

## Recovery and failure semantics

| Failure point | Recovery behavior |
|---|---|
| Control plane stops before publishing | A replacement publisher claims the pending outbox row. |
| Publisher stops after Kafka acknowledgement | The lease expires and the same event ID may be published again. |
| Worker receives a duplicate command | The durable inbox returns the stored result without rerunning the handler. |
| Worker stops after handler completion | A replacement worker publishes the pending result outbox row. |
| Result consumer is offline | Kafka retains the result; the group resumes from its committed offset. |
| Result transaction commits before offset acknowledgement | Redelivery finds the inbox key and does not advance the DAG twice. |
| Broker failure has a known outcome | The claim is released for an immediate later attempt. |
| Broker acknowledgement outcome is unknown | The claim remains fenced until its lease expires, then replays with the stable event ID. |

The design prevents message loss and duplicate state transitions. Task handlers must still use the command event ID as an idempotency key for external side effects because a process can fail while an external system and PostgreSQL disagree.

## Cutover

Production activates `SPRING_PROFILES_ACTIVE=production`, which enables Kafka topic management, control-plane outbox publication, Kafka command dispatch, and result consumption while disabling the in-process dispatcher. Rolling out additional control-plane or worker instances requires unique instance IDs and shared consumer-group names.

Rollback to Phase 2 is an explicit maintenance operation: stop distributed consumers and publishers, confirm no commands or results remain in flight, and then start one in-process dispatcher. FlowForge refuses to run both delivery paths simultaneously.

## Consequences

FlowForge now provides horizontally scalable, restart-safe, at-least-once distributed execution with idempotent state transitions. It accepts the operational cost of Kafka, outbox/inbox storage, and eventual duplicate records in exchange for recoverability and explicit correctness boundaries.

Retries, delayed backoff, deadlines, poison-message DLQs, worker heartbeats, and orphaned running-task recovery remain Phase 4 work.
