# Secret handling and security audit policy

## Secret boundary

- Workflow task configuration must contain only non-sensitive values. Keys that identify passwords, credentials, private keys, API keys, secrets, or tokens are rejected recursively.
- Secrets are represented as named bindings to provider-neutral references: `provider`, `name`, and optional immutable `version`.
- PostgreSQL and task-command events persist only references. A secret value is resolved by a worker immediately before invoking the task handler and is never added to the command or task configuration.
- `env` is the local provider implementation. Production deployments add provider implementations for their secret manager without changing workflow or messaging contracts.
- Worker failure messages are scrubbed using the resolved values and credential-assignment patterns before they reach logs, observations, result events, or PostgreSQL.
- Task handlers must not log `TaskExecutionContext.secrets()`, copy resolved values into results, traces, metrics, configuration, or durable storage, or retain values after the handler returns.

## Audit contract

- Every mutating `/api/v1/**` request records an `ATTEMPTED` event before the operation. If the audit store is unavailable, the mutation fails closed.
- A second append-only event records `SUCCEEDED`, `DENIED`, or `FAILED`. A missing terminal event therefore indicates interruption or loss after admission and is operationally actionable.
- Events contain actor, tenant, semantic action, target, outcome, method, path, status, request ID, and timestamp. Request bodies, query strings, authorization headers, exception messages, task configuration, and secret references are excluded.
- PostgreSQL triggers reject application updates and deletes from `security_audit_event`.
- `GET /api/v1/audit-events` is tenant-scoped, paginated, and restricted to `ADMIN`.

## Retention

- The default online query window is 365 days and is configured with `FLOWFORGE_AUDIT_ONLINE_RETENTION`.
- Audit rows remain immutable in the primary table; FlowForge performs no automatic deletion. Production operators must export records to immutable archival storage before any separately authorized database-retention procedure.
- Redaction and retention behavior is covered by unit and PostgreSQL integration tests. Audit records deliberately contain no free-form request or failure-detail field.
