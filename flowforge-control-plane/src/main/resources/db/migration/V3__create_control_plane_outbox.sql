CREATE TABLE control_plane_outbox (
    id UUID PRIMARY KEY,
    workflow_execution_id UUID NOT NULL REFERENCES workflow_execution(id) ON DELETE CASCADE,
    task_execution_id UUID,
    source_event_id UUID REFERENCES execution_event(id) ON DELETE CASCADE,
    message_kind VARCHAR(30) NOT NULL,
    topic VARCHAR(249) NOT NULL,
    record_key VARCHAR(200) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    schema_version INTEGER NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL,
    claimed_by VARCHAR(200),
    claimed_at TIMESTAMPTZ,
    claimed_until TIMESTAMPTZ,
    claim_token UUID,
    published_at TIMESTAMPTZ,
    last_error VARCHAR(2000),
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT fk_outbox_task
        FOREIGN KEY (task_execution_id, workflow_execution_id)
        REFERENCES task_execution(id, workflow_execution_id) ON DELETE CASCADE,
    CONSTRAINT ck_outbox_message_kind
        CHECK (message_kind IN ('TASK_COMMAND', 'EXECUTION_EVENT')),
    CONSTRAINT ck_outbox_status
        CHECK (status IN ('PENDING', 'IN_FLIGHT', 'PUBLISHED')),
    CONSTRAINT ck_outbox_schema_version CHECK (schema_version > 0),
    CONSTRAINT ck_outbox_attempt_count CHECK (attempt_count >= 0),
    CONSTRAINT ck_outbox_message_reference CHECK (
        (message_kind = 'TASK_COMMAND' AND task_execution_id IS NOT NULL AND source_event_id IS NULL)
        OR (message_kind = 'EXECUTION_EVENT' AND source_event_id = id)
    ),
    CONSTRAINT ck_outbox_claim_state CHECK (
        (status = 'PENDING'
            AND claimed_by IS NULL AND claimed_at IS NULL
            AND claimed_until IS NULL AND claim_token IS NULL AND published_at IS NULL)
        OR (status = 'IN_FLIGHT'
            AND claimed_by IS NOT NULL AND claimed_at IS NOT NULL
            AND claimed_until IS NOT NULL AND claim_token IS NOT NULL AND published_at IS NULL)
        OR (status = 'PUBLISHED'
            AND claimed_by IS NULL AND claimed_at IS NULL
            AND claimed_until IS NULL AND claim_token IS NULL AND published_at IS NOT NULL)
    )
);

CREATE INDEX ix_control_plane_outbox_available
    ON control_plane_outbox(available_at, created_at, id)
    WHERE status = 'PENDING';

CREATE INDEX ix_control_plane_outbox_expired_claim
    ON control_plane_outbox(claimed_until, created_at, id)
    WHERE status = 'IN_FLIGHT';

CREATE INDEX ix_control_plane_outbox_workflow
    ON control_plane_outbox(workflow_execution_id, created_at);
