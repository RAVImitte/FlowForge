CREATE TABLE admission_rate_bucket (
    bucket_key VARCHAR(300) PRIMARY KEY,
    capacity INTEGER NOT NULL,
    refill_tokens INTEGER NOT NULL,
    refill_period_ms BIGINT NOT NULL,
    available_tokens DOUBLE PRECISION NOT NULL,
    last_refill_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    state_version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT ck_admission_rate_bucket_capacity CHECK (capacity BETWEEN 1 AND 1000000),
    CONSTRAINT ck_admission_rate_bucket_refill CHECK (
        refill_tokens BETWEEN 1 AND 1000000 AND refill_period_ms > 0
    ),
    CONSTRAINT ck_admission_rate_bucket_tokens CHECK (
        available_tokens >= 0 AND available_tokens <= capacity
    ),
    CONSTRAINT ck_admission_rate_bucket_time CHECK (updated_at >= last_refill_at),
    CONSTRAINT ck_admission_rate_bucket_version CHECK (state_version >= 0)
);

CREATE INDEX ix_workflow_execution_ready_queue
    ON workflow_execution(workflow_id, workflow_version_id)
    WHERE status = 'RUNNING';

CREATE INDEX ix_task_execution_ready_age
    ON task_execution(status, created_at, id)
    WHERE status = 'READY';
