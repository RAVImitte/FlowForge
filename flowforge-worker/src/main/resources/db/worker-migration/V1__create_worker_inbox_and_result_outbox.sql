CREATE TABLE worker_command_inbox (
    event_id UUID PRIMARY KEY,
    workflow_execution_id UUID NOT NULL,
    task_execution_id UUID NOT NULL,
    task_key VARCHAR(100) NOT NULL,
    task_type VARCHAR(100) NOT NULL,
    expected_state_version BIGINT NOT NULL,
    attempt_number INTEGER NOT NULL,
    command_payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'RECEIVED',
    received_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    completed_by VARCHAR(200),
    result_event_id UUID UNIQUE,
    CONSTRAINT ck_worker_inbox_status CHECK (status IN ('RECEIVED', 'COMPLETED')),
    CONSTRAINT ck_worker_inbox_state_version CHECK (expected_state_version >= 0),
    CONSTRAINT ck_worker_inbox_attempt CHECK (attempt_number > 0),
    CONSTRAINT ck_worker_inbox_completion CHECK (
        (status = 'RECEIVED' AND completed_at IS NULL AND completed_by IS NULL AND result_event_id IS NULL)
        OR (status = 'COMPLETED' AND completed_at IS NOT NULL AND completed_by IS NOT NULL AND result_event_id IS NOT NULL)
    )
);

CREATE INDEX ix_worker_inbox_task
    ON worker_command_inbox(task_execution_id, expected_state_version, attempt_number);

CREATE TABLE worker_result_outbox (
    id UUID PRIMARY KEY,
    command_event_id UUID NOT NULL UNIQUE REFERENCES worker_command_inbox(event_id) ON DELETE CASCADE,
    workflow_execution_id UUID NOT NULL,
    task_execution_id UUID NOT NULL,
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
    CONSTRAINT ck_worker_result_status CHECK (status IN ('PENDING', 'IN_FLIGHT', 'PUBLISHED')),
    CONSTRAINT ck_worker_result_schema_version CHECK (schema_version > 0),
    CONSTRAINT ck_worker_result_attempt_count CHECK (attempt_count >= 0),
    CONSTRAINT ck_worker_result_claim_state CHECK (
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

CREATE INDEX ix_worker_result_available
    ON worker_result_outbox(available_at, created_at, id)
    WHERE status = 'PENDING';

CREATE INDEX ix_worker_result_expired_claim
    ON worker_result_outbox(claimed_until, created_at, id)
    WHERE status = 'IN_FLIGHT';
