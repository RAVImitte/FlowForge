ALTER TABLE worker_command_inbox ADD COLUMN tenant_id VARCHAR(63);
UPDATE worker_command_inbox SET tenant_id = 'local';

ALTER TABLE worker_result_outbox ADD COLUMN tenant_id VARCHAR(63);
UPDATE worker_result_outbox SET tenant_id = 'local';
ALTER TABLE worker_result_outbox
    DROP CONSTRAINT worker_result_outbox_command_event_id_fkey,
    DROP CONSTRAINT worker_result_outbox_command_event_id_key;

ALTER TABLE worker_command_inbox
    DROP CONSTRAINT worker_command_inbox_pkey,
    ALTER COLUMN tenant_id SET NOT NULL,
    ADD CONSTRAINT ck_worker_command_tenant_id
        CHECK (tenant_id ~ '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$'),
    ADD PRIMARY KEY (tenant_id, event_id);
CREATE INDEX ix_worker_inbox_tenant_task
    ON worker_command_inbox(tenant_id, task_execution_id, expected_state_version, attempt_number);

ALTER TABLE worker_result_outbox
    ALTER COLUMN tenant_id SET NOT NULL,
    ADD CONSTRAINT ck_worker_result_tenant_id
        CHECK (tenant_id ~ '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$'),
    ADD CONSTRAINT uq_worker_result_tenant_command UNIQUE (tenant_id, command_event_id),
    ADD CONSTRAINT fk_worker_result_tenant_command
        FOREIGN KEY (tenant_id, command_event_id)
        REFERENCES worker_command_inbox(tenant_id, event_id) ON DELETE CASCADE;
CREATE INDEX ix_worker_result_tenant_available
    ON worker_result_outbox(tenant_id, available_at, created_at, id)
    WHERE status = 'PENDING';
