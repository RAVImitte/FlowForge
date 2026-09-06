# Release pipeline

FlowForge separates merge confidence from release authority. The `CI` workflow runs for pull requests and protected branches. The manually dispatched `Release` workflow is the only automation that can publish images, and it targets the protected `release` environment so repository approval rules can gate credential access.

## Required merge checks

Protect `main` with these eight required job names:

- `Fast tests`
- `Architecture boundaries`
- `Security and tenant isolation`
- `Upgrade and rollback compatibility`
- `PostgreSQL recovery and reconciliation`
- `Full reactor and distributed integration tests`
- `Helm and Kubernetes manifests`
- `Dependencies, SBOM, and container images`

The gates are also available locally through `scripts/invoke-quality-gate.ps1`: `Fast`, `Architecture`, `Security`, `Upgrade`, `Recovery`, `Full`, `Manifests`, and `Sbom`. `Migration` remains an alias for the populated-schema test for compatibility with older automation. Fast tests exclude every Testcontainers suite; the full gate remains authoritative and must run with a Docker-compatible engine. The security gate proves fail-closed production configuration, role and tenant boundaries, append-only audit behavior, tenant quota isolation, bounded DLQ replay, and worker secret redaction. The upgrade gate upgrades a populated V12 database through the current Flyway version and proves the supported V17 application SQL contract remains usable on V18. The recovery gate proves PostgreSQL logical restore, named-point WAL/PITR, Redis reconstruction from durable state, and bounded tenant-safe DLQ replay against real Kafka and PostgreSQL. Manifest validation renders optional autoscaling, trust-store, and credential-rotation branches with immutable placeholder digests.

The supply-chain job fails on fixed high or critical dependency, configuration, secret, and image findings. Trivy's advisory database is mutable, so an accepted finding must be documented through the scanner's supported ignore policy with an owner and expiry; lowering the workflow severity is not an exception mechanism.

## Protected release configuration

Configure the `release` GitHub environment with required reviewers and these encrypted secrets:

| Secret | Purpose |
|---|---|
| `REGISTRY_USERNAME` | Least-privilege identity that can push the selected image namespace |
| `REGISTRY_PASSWORD` | Short-lived registry token where supported |
| `COSIGN_PRIVATE_KEY` | Cosign signing key; keep the corresponding public key in the deployment trust policy |
| `COSIGN_PASSWORD` | Password protecting the Cosign private key |

The Actions used by both workflows are pinned to immutable commit SHAs. On GitHub Enterprise Server, administrators must mirror or allow the listed actions before enabling the workflows.

Dispatch `Release` from `main` with a semantic version, registry host, and lowercase image namespace. The workflow rejects any other ref, validates those inputs, proves the security and tenant boundary plus populated-schema upgrade, application rollback, and restore compatibility, runs the full reactor and manifest checks, completes the bounded 25-workflow distributed smoke profile, and generates an aggregate CycloneDX application SBOM before any image is published.

Each control-plane and worker image is built with maximal BuildKit provenance plus an image SBOM, pushed, rescanned by immutable digest, signed by digest, and given a signed CycloneDX attestation. The workflow then verifies both signatures and attestations with a public key derived from the protected signing key. A failed verification, scan, smoke test, or earlier quality gate prevents a successful release.

## Release boundaries

- Never dispatch from an unreviewed revision. GitHub environment approval authorizes publication; it does not replace code review.
- Deploy by the published digest, never by the mutable semantic-version tag.
- Keep registry retention policies for signatures, attestations, and provenance aligned with image retention.
- Rotate registry and signing credentials through the protected environment. A signing-key rotation requires updating deployment verification policy before the next release.
- Slice 7.6 defines backup, restore, rollback, incident, and DLQ recovery procedures; this pipeline deliberately does not automate destructive production rollback or database restoration.
