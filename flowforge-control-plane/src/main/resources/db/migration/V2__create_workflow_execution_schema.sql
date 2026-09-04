ALTER TABLE workflow_version
    ADD CONSTRAINT uq_workflow_version_identity UNIQUE (id, workflow_id, version_number);

CREATE TABLE workflow_execution (
    id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL,
    workflow_version_id UUID NOT NULL,
    workflow_version_number INTEGER NOT NULL,
    idempotency_key VARCHAR(200) NOT NULL,
    status VARCHAR(20) NOT NULL,
    state_version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    CONSTRAINT fk_execution_workflow
        FOREIGN KEY (workflow_id) REFERENCES workflow_definition(id),
    CONSTRAINT fk_execution_version
        FOREIGN KEY (workflow_version_id, workflow_id, workflow_version_number)
        REFERENCES workflow_version(id, workflow_id, version_number),
    CONSTRAINT uq_execution_idempotency UNIQUE (workflow_id, idempotency_key),
    CONSTRAINT ck_execution_version_number CHECK (workflow_version_number > 0),
    CONSTRAINT ck_execution_state_version CHECK (state_version >= 0),
    CONSTRAINT ck_execution_status
        CHECK (status IN ('PENDING', 'RUNNING', 'CANCELLING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    CONSTRAINT ck_execution_timestamps CHECK (
        (status = 'PENDING' AND started_at IS NULL AND finished_at IS NULL)
        OR (status IN ('RUNNING', 'CANCELLING') AND started_at IS NOT NULL AND finished_at IS NULL)
        OR (status IN ('SUCCEEDED', 'FAILED') AND started_at IS NOT NULL AND finished_at IS NOT NULL)
        OR (status = 'CANCELLED' AND finished_at IS NOT NULL)
    ),
    CONSTRAINT ck_execution_time_order CHECK (
        (started_at IS NULL OR started_at >= created_at)
        AND (finished_at IS NULL OR finished_at >= created_at)
        AND (started_at IS NULL OR finished_at IS NULL OR finished_at >= started_at)
    )
);

CREATE INDEX ix_execution_workflow_created
    ON workflow_execution(workflow_id, created_at DESC);

CREATE INDEX ix_execution_active
    ON workflow_execution(status, created_at)
    WHERE status IN ('PENDING', 'RUNNING', 'CANCELLING');

CREATE TABLE task_execution (
    id UUID PRIMARY KEY,
    workflow_execution_id UUID NOT NULL REFERENCES workflow_execution(id) ON DELETE CASCADE,
    task_key VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    state_version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    CONSTRAINT uq_task_execution_key UNIQUE (workflow_execution_id, task_key),
    CONSTRAINT uq_task_execution_identity UNIQUE (id, workflow_execution_id),
    CONSTRAINT ck_task_execution_state_version CHECK (state_version >= 0),
    CONSTRAINT ck_task_execution_status
        CHECK (status IN ('BLOCKED', 'READY', 'RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'CANCELLED')),
    CONSTRAINT ck_task_execution_timestamps CHECK (
        (status IN ('BLOCKED', 'READY') AND started_at IS NULL AND finished_at IS NULL)
        OR (status = 'RUNNING' AND started_at IS NOT NULL AND finished_at IS NULL)
        OR (status IN ('SUCCEEDED', 'FAILED', 'TIMED_OUT') AND started_at IS NOT NULL AND finished_at IS NOT NULL)
        OR (status = 'CANCELLED' AND finished_at IS NOT NULL)
    ),
    CONSTRAINT ck_task_execution_time_order CHECK (
        (started_at IS NULL OR started_at >= created_at)
        AND (finished_at IS NULL OR finished_at >= created_at)
        AND (started_at IS NULL OR finished_at IS NULL OR finished_at >= started_at)
    )
);

CREATE INDEX ix_task_execution_ready
    ON task_execution(created_at, id)
    WHERE status = 'READY';

CREATE INDEX ix_task_execution_workflow_status
    ON task_execution(workflow_execution_id, status);

CREATE TABLE task_attempt (
    id UUID PRIMARY KEY,
    task_execution_id UUID NOT NULL REFERENCES task_execution(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    error_code VARCHAR(100),
    error_message VARCHAR(2000),
    CONSTRAINT uq_task_attempt_number UNIQUE (task_execution_id, attempt_number),
    CONSTRAINT ck_task_attempt_number CHECK (attempt_number > 0),
    CONSTRAINT ck_task_attempt_status
        CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'CANCELLED')),
    CONSTRAINT ck_task_attempt_timestamps CHECK (
        (status = 'RUNNING' AND finished_at IS NULL)
        OR (status <> 'RUNNING' AND finished_at IS NOT NULL)
    ),
    CONSTRAINT ck_task_attempt_time_order CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE INDEX ix_task_attempt_task
    ON task_attempt(task_execution_id, attempt_number);

CREATE TABLE execution_event (
    id UUID PRIMARY KEY,
    sequence_number BIGSERIAL NOT NULL UNIQUE,
    workflow_execution_id UUID NOT NULL REFERENCES workflow_execution(id) ON DELETE CASCADE,
    task_execution_id UUID,
    event_type VARCHAR(50) NOT NULL,
    from_status VARCHAR(20),
    to_status VARCHAR(20),
    occurred_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT fk_execution_event_task
        FOREIGN KEY (task_execution_id, workflow_execution_id)
        REFERENCES task_execution(id, workflow_execution_id) ON DELETE CASCADE
);

CREATE INDEX ix_execution_event_history
    ON execution_event(workflow_execution_id, sequence_number);
