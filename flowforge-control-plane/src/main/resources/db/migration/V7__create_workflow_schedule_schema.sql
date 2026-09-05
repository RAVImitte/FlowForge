CREATE TABLE workflow_schedule (
    id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES workflow_definition(id),
    schedule_type VARCHAR(20) NOT NULL,
    one_time_at TIMESTAMPTZ,
    cron_expression VARCHAR(200),
    time_zone VARCHAR(100),
    misfire_policy VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    next_fire_at TIMESTAMPTZ NOT NULL,
    lock_version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_workflow_schedule_type CHECK (schedule_type IN ('ONE_TIME', 'CRON')),
    CONSTRAINT ck_workflow_schedule_shape CHECK (
        (schedule_type = 'ONE_TIME' AND one_time_at IS NOT NULL
            AND cron_expression IS NULL AND time_zone IS NULL)
        OR
        (schedule_type = 'CRON' AND one_time_at IS NULL
            AND cron_expression IS NOT NULL AND time_zone IS NOT NULL)
    ),
    CONSTRAINT ck_workflow_schedule_misfire CHECK (misfire_policy IN ('FIRE_ONCE', 'SKIP')),
    CONSTRAINT ck_workflow_schedule_status CHECK (status IN ('ACTIVE', 'PAUSED', 'DELETED')),
    CONSTRAINT ck_workflow_schedule_lock_version CHECK (lock_version >= 0),
    CONSTRAINT ck_workflow_schedule_time_order CHECK (updated_at >= created_at)
);

CREATE INDEX ix_workflow_schedule_due
    ON workflow_schedule(next_fire_at, id)
    WHERE status = 'ACTIVE';

CREATE INDEX ix_workflow_schedule_workflow
    ON workflow_schedule(workflow_id, created_at DESC)
    WHERE status <> 'DELETED';

CREATE TABLE workflow_schedule_trigger (
    id UUID PRIMARY KEY,
    schedule_id UUID NOT NULL REFERENCES workflow_schedule(id),
    scheduled_fire_at TIMESTAMPTZ NOT NULL,
    idempotency_key VARCHAR(200) NOT NULL,
    status VARCHAR(20) NOT NULL,
    workflow_execution_id UUID REFERENCES workflow_execution(id),
    error_message VARCHAR(2000),
    created_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ,
    CONSTRAINT uq_workflow_schedule_fire UNIQUE (schedule_id, scheduled_fire_at),
    CONSTRAINT uq_workflow_schedule_idempotency UNIQUE (idempotency_key),
    CONSTRAINT ck_workflow_schedule_trigger_status
        CHECK (status IN ('PENDING', 'STARTED', 'SKIPPED', 'FAILED')),
    CONSTRAINT ck_workflow_schedule_trigger_processed CHECK (
        (status = 'PENDING' AND processed_at IS NULL)
        OR (status <> 'PENDING' AND processed_at IS NOT NULL)
    )
);

CREATE INDEX ix_workflow_schedule_trigger_history
    ON workflow_schedule_trigger(schedule_id, scheduled_fire_at DESC);
