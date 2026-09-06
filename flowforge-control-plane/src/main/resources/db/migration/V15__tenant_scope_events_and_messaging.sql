ALTER TABLE workflow_execution
    ADD CONSTRAINT uq_workflow_execution_tenant_identity UNIQUE (tenant_id, id);

ALTER TABLE execution_event ADD COLUMN tenant_id VARCHAR(63);
UPDATE execution_event event
   SET tenant_id = execution.tenant_id
  FROM workflow_execution execution
 WHERE execution.id = event.workflow_execution_id;
ALTER TABLE execution_event
    ALTER COLUMN tenant_id SET NOT NULL,
    ADD CONSTRAINT fk_execution_event_tenant
        FOREIGN KEY (tenant_id) REFERENCES tenant_registry(tenant_id),
    ADD CONSTRAINT fk_execution_event_workflow_tenant
        FOREIGN KEY (tenant_id, workflow_execution_id)
        REFERENCES workflow_execution(tenant_id, id) ON DELETE CASCADE,
    ADD CONSTRAINT uq_execution_event_tenant_identity UNIQUE (tenant_id, id);
CREATE INDEX ix_execution_event_tenant_workflow
    ON execution_event(tenant_id, workflow_execution_id, sequence_number);

ALTER TABLE control_plane_outbox ADD COLUMN tenant_id VARCHAR(63);
UPDATE control_plane_outbox outbox
   SET tenant_id = execution.tenant_id
  FROM workflow_execution execution
 WHERE execution.id = outbox.workflow_execution_id;
ALTER TABLE control_plane_outbox
    ALTER COLUMN tenant_id SET NOT NULL,
    ADD CONSTRAINT fk_control_plane_outbox_tenant
        FOREIGN KEY (tenant_id) REFERENCES tenant_registry(tenant_id),
    ADD CONSTRAINT fk_control_plane_outbox_workflow_tenant
        FOREIGN KEY (tenant_id, workflow_execution_id)
        REFERENCES workflow_execution(tenant_id, id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_control_plane_outbox_event_tenant
        FOREIGN KEY (tenant_id, source_event_id)
        REFERENCES execution_event(tenant_id, id) ON DELETE CASCADE;
CREATE INDEX ix_control_plane_outbox_tenant_available
    ON control_plane_outbox(tenant_id, available_at, created_at, id)
    WHERE status = 'PENDING';

ALTER TABLE control_plane_result_inbox ADD COLUMN tenant_id VARCHAR(63);
UPDATE control_plane_result_inbox inbox
   SET tenant_id = execution.tenant_id
  FROM workflow_execution execution
 WHERE execution.id = inbox.workflow_execution_id;
ALTER TABLE control_plane_result_inbox
    DROP CONSTRAINT control_plane_result_inbox_pkey,
    ALTER COLUMN tenant_id SET NOT NULL,
    ADD CONSTRAINT fk_result_inbox_tenant
        FOREIGN KEY (tenant_id) REFERENCES tenant_registry(tenant_id),
    ADD CONSTRAINT fk_result_inbox_workflow_tenant
        FOREIGN KEY (tenant_id, workflow_execution_id)
        REFERENCES workflow_execution(tenant_id, id) ON DELETE CASCADE,
    ADD PRIMARY KEY (tenant_id, consumer_name, event_id);
CREATE INDEX ix_result_inbox_tenant_task
    ON control_plane_result_inbox(tenant_id, task_execution_id, expected_state_version, attempt_number);

ALTER TABLE control_plane_heartbeat_inbox ADD COLUMN tenant_id VARCHAR(63);
UPDATE control_plane_heartbeat_inbox inbox
   SET tenant_id = execution.tenant_id
  FROM workflow_execution execution
 WHERE execution.id = inbox.workflow_execution_id;
ALTER TABLE control_plane_heartbeat_inbox
    DROP CONSTRAINT control_plane_heartbeat_inbox_pkey,
    ALTER COLUMN tenant_id SET NOT NULL,
    ADD CONSTRAINT fk_heartbeat_inbox_tenant
        FOREIGN KEY (tenant_id) REFERENCES tenant_registry(tenant_id),
    ADD CONSTRAINT fk_heartbeat_inbox_workflow_tenant
        FOREIGN KEY (tenant_id, workflow_execution_id)
        REFERENCES workflow_execution(tenant_id, id) ON DELETE CASCADE,
    ADD PRIMARY KEY (tenant_id, consumer_name, event_id);
CREATE INDEX ix_heartbeat_inbox_tenant_task
    ON control_plane_heartbeat_inbox(tenant_id, task_execution_id, attempt_number, received_at DESC);
