# FlowForge Helm deployment

This chart deploys the control plane and workers as independently scalable,
non-root workloads. PostgreSQL, Kafka, Redis, and the OIDC issuer are external
production dependencies; the development services in `compose.yaml` are not
installed by this chart.

## Required secret contract

The chart does not create or accept credential values. Provision these Secrets
through the cluster's external-secret controller or another approved secret
delivery path before installing the release:

| Default Secret | Required keys |
| --- | --- |
| `flowforge-database` | `url`, `username`, `password` |
| `flowforge-kafka` | `bootstrap-servers`, `security-protocol`, `sasl-mechanism`, `sasl-jaas-config` |
| `flowforge-redis` | `host`, `port`, `password` |
| `flowforge-oidc` | `issuer-uri`, `jwk-set-uri` |

For private certificate authorities, set `trustStore.enabled=true` and provide
`flowforge-truststore` with `truststore.p12` and `password` keys. The PKCS12 file
is mounted read-only; its password is sourced through `secretKeyRef`. Secret
names and key mappings can be changed under `secretRefs` and `trustStore`.

## Credential rotation

Use a two-valid-credential overlap whenever the provider supports it: create the
replacement credential, update the externally managed Kubernetes Secret, restart
both workloads, verify new connections, and only then revoke the old credential.
The chart never receives a credential value. Set `global.rotationToken` to a new
opaque revision identifier to change the pod template and trigger a rolling restart:

```powershell
helm upgrade flowforge ./deploy/helm/flowforge `
  --namespace flowforge --reuse-values `
  --set-string global.rotationToken=rotation-2026-09-06-01 `
  --atomic --timeout 10m
```

Never put a password, token, certificate, key, or secret-manager response in the
rotation token; Helm release history and workload annotations expose it as metadata.
Follow the Phase 7 upgrade and credential-rotation runbook for provider ordering,
verification, revocation, and rollback boundaries.

## Build and install

Build local `linux/amd64` images with commit-derived timestamps and OCI metadata:

```powershell
./scripts/build-images.ps1 -Registry registry.example.com/flowforge -Tag 0.1.0
```

The script auto-detects the active JDK truststore and exposes it to the builder
as an ephemeral BuildKit secret. This permits dependency resolution behind a
corporate TLS proxy without baking organization-specific CAs into the image.
Use `-BuildTrustStore <path>` to override detection.

Pass `-Push` only after authenticating the target registry. Release deployments
should set image digests, which take precedence over tags:

```powershell
helm upgrade --install flowforge ./deploy/helm/flowforge `
  --namespace flowforge --create-namespace `
  --set global.imageRegistry=registry.example.com/flowforge `
  --set controlPlane.image.digest=sha256:CONTROL_PLANE_DIGEST `
  --set worker.image.digest=sha256:WORKER_DIGEST `
  --atomic --timeout 10m
```

Run `helm lint --strict` and render both the default and enabled optional paths
before rollout. The chart supplies startup, liveness, and readiness probes,
graceful pre-stop windows, zero-unavailable rolling updates, PodDisruptionBudgets,
resource requests/limits, topology spread, a restricted security context, and a
writable size-bounded `/tmp` volume for otherwise read-only filesystems.

CPU autoscaling is intentionally disabled by default. When enabled, keep worker
replica count, per-pod concurrency, Kafka partition count, and database pool
capacity within the tested Phase 6 capacity envelope. Queue-lag autoscaling is a
deployment-specific integration and is not approximated by this chart.

## Rollout verification and rollback

```powershell
kubectl -n flowforge rollout status deployment/flowforge-flowforge-control-plane --timeout=5m
kubectl -n flowforge rollout status deployment/flowforge-flowforge-worker --timeout=5m
kubectl -n flowforge get pods,pdb,hpa
helm -n flowforge history flowforge
```

Use `helm rollback flowforge <REVISION> --wait --timeout 10m` when readiness or
workflow smoke checks fail. Database migrations are forward-only: image rollback
is permitted only across a compatibility boundary proven before release and never
reverses an applied schema migration.
