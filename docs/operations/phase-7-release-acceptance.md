# Phase 7 release acceptance matrix

This matrix is the final repository acceptance boundary for FlowForge. A release candidate is eligible for publication only when every repository gate passes on the same revision and the protected release workflow subsequently completes its environment-specific controls. Passing this matrix does not itself publish, deploy, restore, or mutate a production environment.

## One-command repository acceptance

Run from the repository root with Java 21, a Docker-compatible engine, Helm, and kubectl available:

```powershell
.\scripts\invoke-release-acceptance.ps1
```

The command stops on the first failure and runs `Security`, `Upgrade`, `Recovery`, `Full`, `Manifests`, and `Sbom` in that order. Evidence is valid only for the exact Git revision and dependency/advisory state that was tested. Never combine successful rows from different revisions into one approval.

## Repository acceptance matrix

| Boundary | Required gate | Pass condition | Principal evidence |
|---|---|---|---|
| Authentication, authorization, secrets, audit, and tenant isolation | `Security` | Production configuration fails closed; role and tenant boundaries hold; audit is append-only/fail-closed; foreign DLQ data is hidden; worker secret failures are redacted | Security/component tests plus real PostgreSQL and Kafka tenant tests |
| Forward schema upgrade and application rollback | `Upgrade` | A populated V12 schema reaches the current version and the explicitly supported V17 application SQL contract remains usable on V18 | `TenantMigrationIntegrationTest`, `SchemaRollbackCompatibilityIntegrationTest` |
| Restore, ephemeral reconstruction, and operator replay | `Recovery` | Logical restore, named-point PITR, Redis reconstruction, and bounded tenant-safe DLQ replay all reconcile without correctness loss | Five real-infrastructure recovery suites |
| Whole-system regression | `Full` | Every module, architecture rule, API contract, persistence test, and distributed Testcontainers scenario passes with zero failures, errors, or skips | Complete Maven `verify` reactor |
| Deployment rendering and security context | `Manifests` | Strict Helm lint succeeds; optional autoscaling/trust/rotation paths render; kubectl client validation accepts the output | Versioned Helm chart and manifest contract tests |
| Application software bill of materials | `Sbom` | A non-empty aggregate CycloneDX 1.6 SBOM is generated without running a partial application test set | `target/sbom/flowforge.cdx.json` |

The authoritative release workflow repeats security, upgrade, recovery, full, manifest, smoke, and SBOM checks before it is allowed to publish images. A gate cannot be waived by editing this document; exceptions require a reviewed code/configuration change with an owner and expiry where the underlying tool supports it.

## Protected publication matrix

These controls require GitHub's protected `release` environment and registry/signing credentials, so they are verified by `.github/workflows/release.yml`, not claimed by a local repository run.

| Boundary | Required release control | Pass condition |
|---|---|---|
| Authority and revision | Manual dispatch from clean `main`, semantic version validation, required environment reviewers | The approved commit and requested version are immutable inputs to the job |
| Distributed smoke | Bounded `single-6p` smoke profile with reset isolated data | 25 workflows reconcile durably with zero generator errors or duplicate commands |
| Image provenance | BuildKit `provenance: mode=max` and image SBOM enabled | Both component images are published by immutable digest with provenance |
| Vulnerability policy | Trivy filesystem and digest-pinned image scans | No fixed HIGH or CRITICAL finding remains unhandled |
| Authenticity and attestation | Protected Cosign key, digest signing, CycloneDX attestation, and immediate verification | Both signatures and both attestations verify against the derived public key |
| Deployment | Digest-pinned Helm values and the deployment runbook | Rollout health, tenant canary, metrics, and rollback observation window pass before promotion |

If any publication control fails, no release is accepted. Do not retag a partially published image as a workaround; correct the cause and rerun the workflow for the same reviewed revision or a new revision.

## Verified closeout evidence

On 2026-09-07, the complete repository matrix passed on one working-tree revision on Windows with Java 21.0.6, Rancher Desktop's Moby-compatible engine (Docker API 1.54, server 29.5.3), Helm 4.2.3, and kubectl 1.35.8. The recorded evidence is:

- `Security`: 24 tests passed, including real PostgreSQL/Kafka isolation and replay coverage;
- `Upgrade`: two populated-schema upgrade and rollback-compatibility tests passed;
- `Recovery`: thirteen logical restore, named-point PITR, Redis reconstruction, compatibility, and replay tests passed;
- `Full`: 282 tests across 92 suites passed with zero failures, errors, or skips;
- `Manifests`: strict Helm lint and kubectl client validation passed for the optional autoscaling, trust-store, and rotation paths;
- `Sbom`: the validated aggregate CycloneDX 1.6 document contains 156 runtime components.

Publication controls remain intentionally unexecuted until a reviewed commit reaches `main` and an authorized reviewer dispatches the protected release workflow.

## Approval and retention

Attach the revision, workflow URLs, gate logs, test totals, generated SBOM digest, image digests, scan database timestamps, smoke report, signatures, attestations, approvers, and decision time to the release record. Retain that evidence with the images for the supported release lifetime. Raw task payloads, credentials, bearer tokens, resolved secrets, and signing material must never enter the evidence bundle.

Phase 7 operational procedures remain defined by the [recovery](phase-7-recovery-runbook.md), [upgrade and credential rotation](phase-7-upgrade-and-credential-rotation-runbook.md), and [incident and DLQ replay](phase-7-incident-and-dlq-replay-runbook.md) runbooks.
