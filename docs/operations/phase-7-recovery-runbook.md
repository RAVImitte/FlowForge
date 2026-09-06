# Phase 7 backup and recovery runbook

## Scope and current evidence

PostgreSQL is the durable correctness boundary. Kafka is an at-least-once transport, Redis is an ephemeral coordination mirror, and telemetry stores are not workflow state. Back up the control-plane and worker schemas together when they share a database.

The automated `Recovery` gate proves both a transaction-consistent logical restore and physical point-in-time recovery. The logical path uses `pg_dump` and `pg_restore` and validates the complete Flyway history, every restored table count, recovery-point exclusion, tenant ownership constraints, append-only audit enforcement, secret-reference preservation, durable permit preservation, and Redis reconstruction from PostgreSQL. The physical path validates a `pg_basebackup` manifest, archives WAL, restores to a named recovery point, promotes onto a new timeline, excludes a post-boundary write, and validates all Flyway migrations. Managed-service backup APIs, object-store retention, encryption, and cross-region restoration remain environment-specific acceptance requirements.

Run the proof locally with Rancher Desktop or another Docker-compatible engine:

```powershell
.\scripts\invoke-quality-gate.ps1 -Gate Recovery
```

## Recovery objectives

Define environment-specific RPO and RTO before deployment. They must be backed by measured restore exercises, not inferred from backup-job success. Record:

- maximum acceptable committed-work loss;
- maximum service restoration time;
- backup and WAL archive frequency;
- retention and legal-hold requirements;
- encryption key owner and rotation process;
- the most recent successful isolated restore time.

A backup is usable only when its checksum, decryption, catalog listing, schema validation, invariant checks, and application smoke test all pass in an isolated environment.

## Backup policy

Production should use encrypted PostgreSQL physical base backups plus continuous WAL archival for PITR. Keep periodic custom-format logical backups as a portable secondary recovery path and migration diagnostic. Store backups outside the database failure domain with immutable retention, checksums, encryption-key separation, and access audit logs.

For a logical recovery point, use credentials supplied by the secret manager and never place a password in shell history:

```bash
pg_dump --format=custom --compress=6 --no-owner --no-privileges \
  --dbname="$FLOWFORGE_DATABASE_URL" --file=flowforge.dump
pg_restore --list flowforge.dump > flowforge.dump.catalog
sha256sum flowforge.dump > flowforge.dump.sha256
```

Capture the backup time, PostgreSQL major/minor version, Flyway versions for both schemas, image digests, checksum, encryption-key identifier, and operator/job identity in the backup manifest. Never include database credentials, task payloads, resolved secrets, or raw secret-manager responses.

## Physical PITR contract

Enable `wal_level=replica`, `archive_mode=on`, and a fail-closed `archive_command` that durably copies each WAL segment outside the database failure domain. Take a physical base backup with a manifest and verify it before accepting it:

```bash
pg_basebackup --format=tar --wal-method=stream --manifest-checksums=SHA256 \
  --checkpoint=fast --pgdata=/backup
pg_verifybackup /backup/extracted
```

For a named operational boundary, record `SELECT pg_create_restore_point('approved_name')` with the incident or release evidence and retain every required WAL segment. Restore the base backup into an isolated instance, restore its streamed `pg_wal`, make the archive available, and configure:

```conf
restore_command = 'cp /archive/%f %p'
recovery_target_name = 'approved_name'
recovery_target_action = 'promote'
```

Create `recovery.signal`, start PostgreSQL, and do not admit traffic until recovery has promoted, the timeline has advanced, the selected-boundary invariants pass, and post-boundary rows are absent. A named restore point is useful only when its WAL record and all preceding required WAL remain available.

## Conservative restore sequence

1. Declare an incident, stop new workflow and schedule admission, and record the chosen recovery point and affected tenants.
2. Stop control-plane and worker replicas. Preserve logs, metrics snapshots, Kafka offsets, image digests, and backup/WAL manifests.
3. Restore into a new isolated PostgreSQL instance of the same major version. Never overwrite the damaged primary as the first recovery attempt.
4. Validate the backup checksum and catalog before restoration. Restore atomically:

   ```bash
   createdb flowforge_restore
   pg_restore --single-transaction --exit-on-error --no-owner --no-privileges \
     --dbname=flowforge_restore flowforge.dump
   ```

5. Run Flyway validation without applying a newer application migration. Confirm both schema histories match the selected application images.
6. Check tenant roots, workflow/version/execution references, task/attempt references, inbox/outbox rows, schedule triggers, active permits, quota rows, and append-only audit objects. Compare durable counts with the backup manifest.
7. Never restore Redis from a backup. Start with an empty FlowForge Redis namespace and allow the PostgreSQL-authoritative reconciliation paths to reconstruct permits and rate-bucket mirrors.
8. Do not attach the restored applications to the old Kafka transport until the database recovery point and Kafka record/offset window have been reconciled. For a disaster restore, prefer a clean replacement transport and allow durable outboxes, leases, retries, and fencing to rebuild delivery. Reusing old topics without a reviewed offset plan can introduce records committed after the database recovery point.
9. Start one control-plane replica with scheduling and dispatch disabled. Validate readiness, Flyway state, audit writes, and tenant-scoped reads.
10. Enable reconciliation and reapers, then start one worker. Observe expired leases, pending outboxes, duplicate suppression, DLQs, Redis reconstruction, and active-execution age before scaling out.
11. Run a bounded tenant-scoped smoke workflow with a new idempotency key. Confirm API state, durable terminal state, event history, outbox drainage, zero duplicate terminal transitions, and expected Kafka lag.
12. Resume schedules and ingress gradually. Record actual RPO/RTO, unresolved workflows, replay decisions, approvers, and evidence locations.

## Abort conditions

Do not promote the restored environment if any Flyway checksum differs, a required table or trigger is missing, a tenant reference is orphaned, security audit rows are mutable, restored counts do not match the manifest, Redis cannot be rebuilt from durable rows, an old Kafka record crosses the selected recovery boundary unexpectedly, or the smoke workflow fails durable reconciliation.

Do not repair these failures with direct row edits, manual Redis keys, deleted Kafka offsets, or fabricated inbox/outbox records. Preserve the failed restore for analysis and repeat from a known-good recovery point.

## Evidence and ownership

Attach the backup manifest, checksums, restore logs, Flyway validation, invariant-query output, application image digests, Kafka offset decision, Redis reconstruction metrics, smoke report, RPO/RTO measurement, and incident approvals to the recovery record. A different operator should review the evidence before traffic resumes.

Incident response and bounded tenant-safe replay are covered by the [incident and DLQ replay runbook](phase-7-incident-and-dlq-replay-runbook.md). The complete operational acceptance matrix and ADR-007 remain for the final Slice 7.6 closeout. Upgrade/rollback boundaries and credential rotation are covered by the companion Phase 7 runbook.
