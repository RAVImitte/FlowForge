CREATE TABLE control_plane_heartbeat_inbox (
    consumer_name VARCHAR(100) NOT NULL,
    event_id UUID NOT NULL,
    workflow_execution_id UUID NOT NULL REFERENCES workflow_execution(id) ON DELETE CASCADE,
    task_execution_id UUID NOT NULL,
    task_key VARCHAR(100) NOT NULL,
    attempt_number INTEGER NOT NULL,
    fencing_token UUID NOT NULL,
    worker_id VARCHAR(200) NOT NULL,
    payload JSONB NOT NULL,
    disposition VARCHAR(20) NOT NULL DEFAULT 'PROCESSING',
    received_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ,
    PRIMARY KEY (consumer_name, event_id),
    CONSTRAINT fk_heartbeat_inbox_task
        FOREIGN KEY (task_execution_id, workflow_execution_id)
        REFERENCES task_execution(id, workflow_execution_id) ON DELETE CASCADE,
    CONSTRAINT ck_heartbeat_inbox_attempt CHECK (attempt_number > 0),
    CONSTRAINT ck_heartbeat_inbox_disposition
        CHECK (disposition IN ('PROCESSING', 'APPLIED', 'STALE')),
    CONSTRAINT ck_heartbeat_inbox_processed_state CHECK (
        (disposition = 'PROCESSING' AND processed_at IS NULL)
        OR (disposition IN ('APPLIED', 'STALE') AND processed_at IS NOT NULL)
    )
);

CREATE INDEX ix_heartbeat_inbox_task
    ON control_plane_heartbeat_inbox(task_execution_id, attempt_number, received_at DESC);
