CREATE TABLE dlq_replay_request (
    tenant_id varchar(63) NOT NULL REFERENCES tenant_registry(tenant_id),
    idempotency_key uuid NOT NULL,
    dlq_topic varchar(200) NOT NULL,
    dlq_partition integer NOT NULL,
    dlq_offset bigint NOT NULL,
    dlq_record_id varchar(300) NOT NULL,
    source_topic varchar(200) NOT NULL,
    requested_by varchar(200) NOT NULL,
    reason varchar(1000) NOT NULL,
    status varchar(20) NOT NULL,
    attempt_count integer NOT NULL,
    lease_expires_at timestamptz,
    failure_class varchar(500),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    published_at timestamptz,
    PRIMARY KEY (tenant_id, idempotency_key),
    CONSTRAINT uq_dlq_replay_source UNIQUE (tenant_id, dlq_topic, dlq_partition, dlq_offset),
    CONSTRAINT ck_dlq_replay_location CHECK (dlq_partition >= 0 AND dlq_offset >= 0),
    CONSTRAINT ck_dlq_replay_reason CHECK (char_length(reason) BETWEEN 10 AND 1000),
    CONSTRAINT ck_dlq_replay_status CHECK (status IN ('PUBLISHING', 'PUBLISHED', 'FAILED')),
    CONSTRAINT ck_dlq_replay_attempts CHECK (attempt_count >= 1),
    CONSTRAINT ck_dlq_replay_state CHECK (
        (status = 'PUBLISHING' AND lease_expires_at IS NOT NULL AND published_at IS NULL)
        OR (status = 'PUBLISHED' AND lease_expires_at IS NULL AND published_at IS NOT NULL)
        OR (status = 'FAILED' AND lease_expires_at IS NULL AND published_at IS NULL)
    ),
    CONSTRAINT ck_dlq_replay_time_order CHECK (
        updated_at >= created_at AND (published_at IS NULL OR published_at >= created_at)
    )
);

CREATE INDEX ix_dlq_replay_status_lease
    ON dlq_replay_request(status, lease_expires_at)
    WHERE status = 'PUBLISHING';

CREATE INDEX ix_dlq_replay_tenant_created
    ON dlq_replay_request(tenant_id, created_at DESC, idempotency_key);
