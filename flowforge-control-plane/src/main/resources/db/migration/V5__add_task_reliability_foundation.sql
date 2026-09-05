ALTER TABLE workflow_task
    ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN initial_backoff_ms BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN backoff_multiplier DOUBLE PRECISION NOT NULL DEFAULT 2.0,
    ADD COLUMN max_backoff_ms BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN jitter_factor DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    ADD COLUMN attempt_timeout_ms BIGINT,
    ADD COLUMN retryable_error_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD CONSTRAINT ck_workflow_task_max_attempts
        CHECK (max_attempts BETWEEN 1 AND 100),
    ADD CONSTRAINT ck_workflow_task_initial_backoff
        CHECK (initial_backoff_ms >= 0),
    ADD CONSTRAINT ck_workflow_task_backoff_multiplier
        CHECK (backoff_multiplier >= 1.0 AND backoff_multiplier < 'Infinity'::double precision),
    ADD CONSTRAINT ck_workflow_task_max_backoff
        CHECK (max_backoff_ms >= 0 AND (max_attempts = 1 OR max_backoff_ms >= initial_backoff_ms)),
    ADD CONSTRAINT ck_workflow_task_jitter
        CHECK (jitter_factor >= 0.0 AND jitter_factor <= 1.0),
    ADD CONSTRAINT ck_workflow_task_attempt_timeout
        CHECK (attempt_timeout_ms IS NULL OR attempt_timeout_ms > 0),
    ADD CONSTRAINT ck_workflow_task_retryable_codes
        CHECK (jsonb_typeof(retryable_error_codes) = 'array'
            AND jsonb_array_length(retryable_error_codes) <= 100);

ALTER TABLE task_execution
    ADD COLUMN next_attempt_at TIMESTAMPTZ;

ALTER TABLE task_execution DROP CONSTRAINT ck_task_execution_status;
ALTER TABLE task_execution
    ADD CONSTRAINT ck_task_execution_status
        CHECK (status IN (
            'BLOCKED', 'READY', 'RUNNING', 'RETRY_SCHEDULED',
            'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'CANCELLED'
        ));

ALTER TABLE task_execution DROP CONSTRAINT ck_task_execution_timestamps;
ALTER TABLE task_execution
    ADD CONSTRAINT ck_task_execution_timestamps CHECK (
        (status = 'BLOCKED' AND started_at IS NULL AND finished_at IS NULL AND next_attempt_at IS NULL)
        OR (status = 'READY' AND finished_at IS NULL AND next_attempt_at IS NULL)
        OR (status = 'RUNNING' AND started_at IS NOT NULL AND finished_at IS NULL AND next_attempt_at IS NULL)
        OR (status = 'RETRY_SCHEDULED' AND started_at IS NOT NULL AND finished_at IS NULL AND next_attempt_at IS NOT NULL)
        OR (status IN ('SUCCEEDED', 'FAILED', 'TIMED_OUT')
            AND started_at IS NOT NULL AND finished_at IS NOT NULL AND next_attempt_at IS NULL)
        OR (status = 'CANCELLED' AND finished_at IS NOT NULL AND next_attempt_at IS NULL)
    );

ALTER TABLE task_execution DROP CONSTRAINT ck_task_execution_time_order;
ALTER TABLE task_execution
    ADD CONSTRAINT ck_task_execution_time_order CHECK (
        (started_at IS NULL OR started_at >= created_at)
        AND (finished_at IS NULL OR finished_at >= created_at)
        AND (next_attempt_at IS NULL OR next_attempt_at >= created_at)
        AND (started_at IS NULL OR finished_at IS NULL OR finished_at >= started_at)
        AND (started_at IS NULL OR next_attempt_at IS NULL OR next_attempt_at >= started_at)
    );

CREATE INDEX ix_task_execution_retry_due
    ON task_execution(next_attempt_at, id)
    WHERE status = 'RETRY_SCHEDULED';

ALTER TABLE task_attempt
    ADD COLUMN attempt_deadline TIMESTAMPTZ,
    ADD COLUMN lease_deadline TIMESTAMPTZ,
    ADD COLUMN fencing_token UUID,
    ADD CONSTRAINT ck_task_attempt_deadlines CHECK (
        (attempt_deadline IS NULL OR attempt_deadline >= started_at)
        AND (lease_deadline IS NULL OR lease_deadline >= started_at)
    );

CREATE INDEX ix_task_attempt_deadline
    ON task_attempt(attempt_deadline, task_execution_id)
    WHERE status = 'RUNNING' AND attempt_deadline IS NOT NULL;

CREATE INDEX ix_task_attempt_lease
    ON task_attempt(lease_deadline, task_execution_id)
    WHERE status = 'RUNNING' AND lease_deadline IS NOT NULL;
