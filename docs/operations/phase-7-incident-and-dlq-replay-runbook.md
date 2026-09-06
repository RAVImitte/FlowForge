# Phase 7 incident response and tenant-safe DLQ replay

This runbook covers production incident coordination and the bounded replay of Kafka transport dead-letter records. PostgreSQL remains the durable workflow-state authority. A replay republishes an original transport record; it does not authorize direct workflow-state repair.

## Roles and incident lifecycle

- The incident commander owns severity, communications, decision logging, and closure.
- The operations lead owns containment, evidence collection, and platform recovery.
- The tenant or service owner confirms the underlying fault is corrected and approves replay scope.
- A FlowForge administrator performs replay with a tenant-bound JWT. Viewer and operator roles cannot inspect or replay dead-letter records.

Declare an incident when durable queues stop draining, a DLQ alert fires, tenant isolation is suspected, or recovery exceeds the relevant SLO. Record the start time, affected tenants and workflows, dashboards and traces, dependency health, deploy/configuration changes, and the incident commander. Treat suspected cross-tenant exposure as a security incident and stop replay immediately.

## Containment and diagnosis

1. Freeze deployments, credential rotation, schema changes, and all DLQ replay for the affected path.
2. Preserve PostgreSQL, Kafka, application logs, traces, alert notifications, and deployment metadata. Do not edit workflow tables, delete DLQ records, or advance consumer offsets manually.
3. Determine whether the fault is in PostgreSQL, Kafka, Redis, an external task dependency, a message contract, or the deployed application. Redis loss alone is not a reason to restore durable state.
4. Bound the blast radius by tenant, source topic, partition, source offset, deterministic DLQ record ID, event ID, and failure class.
5. Correct the root cause and prove that normal canary traffic succeeds before approving a replay.

For database loss or corruption, follow [the recovery runbook](phase-7-recovery-runbook.md). For application rollback or credential replacement, follow [the upgrade and rotation runbook](phase-7-upgrade-and-credential-rotation-runbook.md).

## Safe inspection contract

The administrative API addresses exactly one retained record by DLQ topic, partition, and offset. Only FlowForge's three versioned command, result, and heartbeat DLQs are accepted. A foreign-tenant record is returned as `404`, indistinguishable from a missing record.

Inspection returns routing metadata, stable message headers, failure metadata, payload byte length, and a SHA-256 digest. It never returns the raw message value. Do not copy payloads, secrets, or resolved credentials into tickets, replay reasons, logs, or audit notes.

```powershell
$api = "https://flowforge.example.com"
$dlqTopic = "flowforge.task.results.dlq.v1"
$partition = 0
$offset = 42
$recordUrl = "$api/api/v1/dead-letters/$dlqTopic/partitions/$partition/offsets/$offset"

curl.exe --fail-with-body `
  -H "Authorization: Bearer $env:FLOWFORGE_ADMIN_TOKEN" `
  $recordUrl
```

Confirm the JWT tenant, source topic, deterministic record ID, tenant ID, event/correlation IDs, schema version, failure class, failure time, byte count, and digest. Abort if any value differs from the incident evidence or if the source consumer is still unhealthy.

## Bounded replay procedure

Replay is deliberately single-record and same-partition. The service preserves the original key/value and stable contract, tenant, correlation, event, and trace headers; strips DLQ-only and exception headers; maps the DLQ through a fixed source-topic allowlist; and waits for Kafka broker acknowledgement.

1. Obtain tenant-owner approval for this exact DLQ topic/partition/offset and record a payload-free reason.
2. Generate one UUID idempotency key and retain it in the incident record. Reuse that key for retries of the same operator action.
3. Submit the replay once. Never script an unbounded partition or topic replay.
4. Verify the receipt, consumer health, workflow state, duplicate-handling metrics, DLQ/retry age, and the security audit attempt/outcome pair.

```powershell
$idempotencyKey = [guid]::NewGuid().ToString()
$body = '{"reason":"dependency recovered and tenant owner approved replay"}'

curl.exe --fail-with-body -X POST `
  -H "Authorization: Bearer $env:FLOWFORGE_ADMIN_TOKEN" `
  -H "Idempotency-Key: $idempotencyKey" `
  -H "Content-Type: application/json" `
  --data $body `
  "$recordUrl/replay"
```

`PUBLISHED` means Kafka acknowledged the source-topic publication and the ledger was marked published. `ALREADY_PUBLISHED` means the durable idempotency ledger suppressed another publication. `409` means another claim still owns the bounded publishing lease; wait for the lease and investigate rather than changing keys. `503` means inspection, Kafka publication, or ledger completion was unavailable. `404` must not be used to infer another tenant's data.

There is an unavoidable at-least-once ambiguity if the process fails after Kafka acknowledges the replay but before PostgreSQL records `PUBLISHED`. A later lease-holder may publish the same original event again. Stable event IDs and downstream inbox/idempotency constraints are the correctness boundary; operators must expect a duplicate delivery and verify that it produced no duplicate state transition.

## Abort and rollback conditions

Stop replay and re-enter containment if the tenant or digest does not match, schema compatibility is uncertain, the source consumer is unhealthy, broker acknowledgement times out, cross-tenant behavior is suspected, duplicate delivery changes durable state twice, or queue/record age rises after the canary. Replaying cannot be undone; compensate through the owning workflow's supported business operation, never by deleting durable events.

## Monitoring and closeout evidence

Monitor `flowforge_kafka_dlq_replay_published_total` and `flowforge_kafka_dlq_replay_failures_total` by source topic together with existing DLQ publication, record-age, inbox duplicate, outbox, consumer-lag, task retry, and workflow-completion signals.

Close the incident only after normal traffic and the approved replay reconcile with PostgreSQL, queues drain, no tenant boundary was crossed, alerts recover, and the audit history contains the administrator, tenant, exact DLQ location, request ID, reason in the replay ledger, and attempt/outcome timestamps. Record the root cause, corrective action, replay receipts and idempotency keys, dashboards/traces, recovery timing, follow-up owner, and prevention work without retaining raw payloads.

## Automated acceptance

Run the independent recovery gate before release and after changing replay, Kafka headers, tenant identity, or the ledger:

```powershell
.\scripts\invoke-quality-gate.ps1 -Gate Recovery
```

`DeadLetterReplayIntegrationTest` uses real PostgreSQL and Kafka containers to prove that a foreign tenant cannot inspect a record, a tenant-owned record replays to its allowlisted source partition, DLQ-only headers are removed, the payload is not persisted in the ledger, and reuse of an idempotency key does not publish a second record.
