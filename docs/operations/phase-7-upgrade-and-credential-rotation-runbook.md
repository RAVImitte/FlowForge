# Phase 7 upgrade, rollback, and credential-rotation runbook

## Release boundary

FlowForge database migrations are forward-only. Never use `helm rollback`, an
older application image, or manual DDL to undo an applied Flyway migration. An
image rollback is allowed only when the release evidence proves that the previous
application version can operate against the newer schema.

The automated `Recovery` gate currently certifies one explicit boundary: a V17
application SQL contract can read, insert, and update workflow tasks after the
additive V18 migration. V18 backfills `secret_references` with an empty JSON object
and gives new legacy writes the same default. This evidence does not certify any
other version pair.

Before an upgrade:

1. Record current image digests, Helm revision, Flyway version, Kafka consumer
   offsets, and the latest verified backup/PITR evidence.
2. Verify the exact N-1 to N compatibility test in CI. Reject destructive or
   contract-changing DDL until an expand/migrate/contract sequence spans releases.
3. Pause schema-concurrent maintenance and ensure only one controlled migration
   owner can run Flyway. PostgreSQL's Flyway history lock serializes accidental
   peers, but it is not a release-approval mechanism.
4. Deploy N, validate readiness and a bounded tenant-scoped smoke workflow, then
   progressively restore traffic while watching errors, lag, leases, outboxes,
   active-execution age, and database saturation.

If N fails after an additive migration, stop new admission and roll application
images back to the certified N-1 digest while leaving the schema at N. If the pair
was not certified, restore service forward with a corrected N image. Use database
recovery only for data loss or corruption, not ordinary application rollback.

## Credential rotation invariant

Prefer a two-valid-credential overlap. At least one credential must remain valid
throughout replacement, pod restart, connection establishment, health checks, and
smoke verification. FlowForge reads Kubernetes Secret-backed environment values at
process startup, so changing a Secret alone does not refresh a running process.

Never put secret material in the rotation token. `global.rotationToken` is an
opaque, non-sensitive revision identifier stored in Helm history and a pod-template
annotation. After the external secret controller has reconciled the replacement:

```powershell
helm upgrade flowforge ./deploy/helm/flowforge `
  --namespace flowforge --reuse-values `
  --set-string global.rotationToken=rotation-2026-09-06-01 `
  --atomic --timeout 10m
kubectl -n flowforge rollout status deployment/flowforge-flowforge-control-plane --timeout=5m
kubectl -n flowforge rollout status deployment/flowforge-flowforge-worker --timeout=5m
```

Use a fresh token for every rotation attempt. Confirm all old pods are gone and
new database, Kafka, Redis, OIDC, or TLS connections succeed before revocation.

## Provider procedures

- PostgreSQL: create a second login or password accepted during overlap; grant the
  minimum existing privileges; update the external Secret; restart and verify pool
  acquisition plus transactional workflow state; then disable the old credential.
- Kafka: add the replacement SASL principal/credential and equivalent topic/group
  ACLs; restart consumers and producers; verify group stability, produce/consume,
  outbox drainage, and DLQ access; then revoke the old principal.
- Redis: use provider-supported dual users or ACL credentials. Verify permit and
  rate-mirror reconstruction after restart before removing the old user. Never
  rotate by restoring Redis state.
- OIDC: publish the new signing key in JWKS before issuing tokens with it. Retain
  the old public key beyond the maximum token lifetime plus cache skew, verify both
  generations, and only then remove the old key. Issuer/JWKS endpoint credential or
  URI changes require the same workload restart.
- PKCS12 trust material: include old and new CA chains during overlap, replace the
  externally managed Secret, restart, and verify every TLS dependency before
  removing the old chain in a later rotation.
- Registry pull and release-signing credentials are delivery-plane credentials;
  rotate them in the registry or CI secret store, verify a digest pull/sign/verify
  operation, then revoke the previous credential. They are not injected into
  FlowForge runtime pods.

## Abort and evidence

Abort revocation if any replica is still old, readiness fails, authentication errors
increase, Kafka groups churn, the outbox stops draining, Redis reconstruction fails,
or the smoke workflow does not reach one durable terminal state. Keep both
credentials valid, restore the prior external Secret version, set a new opaque
rotation token, and roll the workloads again. Do not use a Helm rollback to recover
deleted provider credentials.

Record provider-side credential identifiers (never values), Secret resource
versions, rotation-token revision, pod UIDs/start times, image digests, rollout and
smoke output, authentication metrics, old-credential revocation time, operators,
and approvals in the change record.
