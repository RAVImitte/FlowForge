CREATE TABLE control_plane_result_inbox (
    consumer_name VARCHAR(100) NOT NULL,
    event_id UUID NOT NULL,
    workflow_execution_id UUID NOT NULL REFERENCES workflow_execution(id) ON DELETE CASCADE,
    task_execution_id UUID NOT NULL,
    task_key VARCHAR(100) NOT NULL,
    expected_state_version BIGINT NOT NULL,
    attempt_number INTEGER NOT NULL,
    outcome VARCHAR(20) NOT NULL,
    payload JSONB NOT NULL,
    disposition VARCHAR(20) NOT NULL DEFAULT 'PROCESSING',
    received_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ,
    PRIMARY KEY (consumer_name, event_id),
    CONSTRAINT fk_result_inbox_task
        FOREIGN KEY (task_execution_id, workflow_execution_id)
        REFERENCES task_execution(id, workflow_execution_id) ON DELETE CASCADE,
    CONSTRAINT ck_result_inbox_state_version CHECK (expected_state_version > 0),
    CONSTRAINT ck_result_inbox_attempt CHECK (attempt_number > 0),
    CONSTRAINT ck_result_inbox_outcome
        CHECK (outcome IN ('SUCCEEDED', 'FAILED', 'TIMED_OUT', 'CANCELLED')),
    CONSTRAINT ck_result_inbox_disposition
        CHECK (disposition IN ('PROCESSING', 'APPLIED', 'REDUNDANT')),
    CONSTRAINT ck_result_inbox_processed_state CHECK (
        (disposition = 'PROCESSING' AND processed_at IS NULL)
        OR (disposition IN ('APPLIED', 'REDUNDANT') AND processed_at IS NOT NULL)
    )
);

CREATE INDEX ix_result_inbox_task
    ON control_plane_result_inbox(task_execution_id, expected_state_version, attempt_number);
