CREATE TABLE tenant_quota (
    tenant_id VARCHAR(63) PRIMARY KEY
        REFERENCES tenant_registry(tenant_id),
    quota_version BIGINT NOT NULL,
    max_active_executions INTEGER NOT NULL,
    max_running_tasks INTEGER NOT NULL,
    max_pending_schedule_fires INTEGER NOT NULL,
    max_ready_tasks INTEGER NOT NULL,
    schedule_rate_capacity INTEGER NOT NULL,
    schedule_rate_refill_tokens INTEGER NOT NULL,
    schedule_rate_refill_period_ms BIGINT NOT NULL,
    dispatch_rate_capacity INTEGER NOT NULL,
    dispatch_rate_refill_tokens INTEGER NOT NULL,
    dispatch_rate_refill_period_ms BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_tenant_quota_version CHECK (quota_version >= 1),
    CONSTRAINT ck_tenant_quota_resource_limits CHECK (
        max_active_executions BETWEEN 1 AND 1000000
        AND max_running_tasks BETWEEN 1 AND 1000000
        AND max_pending_schedule_fires BETWEEN 1 AND 1000000
        AND max_ready_tasks BETWEEN 1 AND 1000000
    ),
    CONSTRAINT ck_tenant_quota_schedule_rate CHECK (
        schedule_rate_capacity BETWEEN 1 AND 1000000
        AND schedule_rate_refill_tokens BETWEEN 1 AND 1000000
        AND schedule_rate_refill_period_ms >= 1
    ),
    CONSTRAINT ck_tenant_quota_dispatch_rate CHECK (
        dispatch_rate_capacity BETWEEN 1 AND 1000000
        AND dispatch_rate_refill_tokens BETWEEN 1 AND 1000000
        AND dispatch_rate_refill_period_ms >= 1
    ),
    CONSTRAINT ck_tenant_quota_time_order CHECK (updated_at >= created_at)
);

CREATE INDEX ix_tenant_quota_updated
    ON tenant_quota(updated_at DESC, tenant_id);
