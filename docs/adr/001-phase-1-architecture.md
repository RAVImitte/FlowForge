# ADR-001: Phase 1 architecture

- Status: Accepted
- Date: 2026-08-28

## Context

FlowForge must grow into a distributed orchestration platform without making its correctness depend on Kafka or Redis. Phase 1 needs a small deployable surface while preserving boundaries needed by later orchestrator, worker, and scheduler processes.

## Decision

Use a Maven multi-module clean architecture:

1. A framework-free domain module owns workflow graph invariants.
2. An application module owns use cases and persistence ports.
3. A Spring Boot control-plane module owns HTTP and PostgreSQL adapters.
4. PostgreSQL is the durable source of truth.
5. Workflow definitions are versioned. Published versions are immutable.
6. Existing-resource mutations require an `If-Match` ETag.
7. Archive replaces physical deletion.

Spring JDBC is used instead of an ORM so later phases can express explicit locking, compare-and-set transitions, and `FOR UPDATE SKIP LOCKED` scheduling queries without abstraction leakage.

## Consequences

Phase 2 can add state-machine and DAG execution modules without changing definition semantics. Kafka and Redis remain outside Phase 1, avoiding infrastructure that does not yet serve an execution path. Explicit SQL requires more adapter code, but concurrency and transaction boundaries remain visible and testable.
