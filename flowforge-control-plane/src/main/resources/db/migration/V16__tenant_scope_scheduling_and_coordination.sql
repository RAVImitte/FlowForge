-- Close the rolling-upgrade window opened in V13. Every current root writer
-- supplies tenant ownership explicitly before these defaults are removed.
ALTER TABLE workflow_definition ALTER COLUMN tenant_id DROP DEFAULT;
ALTER TABLE workflow_execution ALTER COLUMN tenant_id DROP DEFAULT;
ALTER TABLE workflow_schedule ALTER COLUMN tenant_id DROP DEFAULT;

ALTER TABLE workflow_schedule
    DROP CONSTRAINT workflow_schedule_workflow_id_fkey,
    ADD CONSTRAINT uq_workflow_schedule_tenant_identity UNIQUE (tenant_id, id),
    ADD CONSTRAINT fk_workflow_schedule_workflow_tenant
        FOREIGN KEY (tenant_id, workflow_id)
        REFERENCES workflow_definition(tenant_id, id);

ALTER TABLE workflow_schedule_trigger ADD COLUMN tenant_id VARCHAR(63);
UPDATE workflow_schedule_trigger trigger
   SET tenant_id = schedule.tenant_id
  FROM workflow_schedule schedule
 WHERE schedule.id = trigger.schedule_id;
ALTER TABLE workflow_schedule_trigger
    DROP CONSTRAINT workflow_schedule_trigger_schedule_id_fkey,
    DROP CONSTRAINT fk_workflow_schedule_trigger_workflow,
    DROP CONSTRAINT workflow_schedule_trigger_workflow_execution_id_fkey,
    ALTER COLUMN tenant_id SET NOT NULL,
    ADD CONSTRAINT fk_schedule_trigger_tenant
        FOREIGN KEY (tenant_id) REFERENCES tenant_registry(tenant_id),
    ADD CONSTRAINT fk_schedule_trigger_schedule_tenant
        FOREIGN KEY (tenant_id, schedule_id)
        REFERENCES workflow_schedule(tenant_id, id),
    ADD CONSTRAINT fk_schedule_trigger_workflow_tenant
        FOREIGN KEY (tenant_id, workflow_id)
        REFERENCES workflow_definition(tenant_id, id),
    ADD CONSTRAINT fk_schedule_trigger_execution_tenant
        FOREIGN KEY (tenant_id, workflow_execution_id)
        REFERENCES workflow_execution(tenant_id, id),
    ADD CONSTRAINT uq_schedule_trigger_tenant_identity UNIQUE (tenant_id, id);
CREATE INDEX ix_schedule_trigger_tenant_pending
    ON workflow_schedule_trigger(tenant_id, available_at, scheduled_fire_at, id)
    WHERE status IN ('PENDING', 'PROCESSING');

ALTER TABLE coordination_resource ADD COLUMN tenant_id VARCHAR(63);
ALTER TABLE coordination_permit ADD COLUMN tenant_id VARCHAR(63);

-- Recover ownership for permits attached to running workflow executions or
-- task attempts. Older generic coordination rows remain owned by local.
UPDATE coordination_permit permit
   SET tenant_id = execution.tenant_id
  FROM workflow_execution execution
 WHERE execution.concurrency_permit_token = permit.token;
UPDATE coordination_permit permit
   SET tenant_id = execution.tenant_id
  FROM task_attempt attempt
  JOIN task_execution task ON task.id = attempt.task_execution_id
  JOIN workflow_execution execution ON execution.id = task.workflow_execution_id
 WHERE attempt.concurrency_permit_token = permit.token;
UPDATE coordination_permit SET tenant_id = 'local' WHERE tenant_id IS NULL;
UPDATE coordination_resource resource
   SET tenant_id = COALESCE(
       (SELECT MIN(permit.tenant_id)
          FROM coordination_permit permit
         WHERE permit.resource_key = resource.resource_key),
       'local'
   );

ALTER TABLE coordination_permit
    DROP CONSTRAINT coordination_permit_resource_key_fkey;
DROP INDEX uq_coordination_permit_active_holder;
DROP INDEX ix_coordination_permit_active_expiry;
ALTER TABLE coordination_resource
    DROP CONSTRAINT coordination_resource_pkey,
    ALTER COLUMN tenant_id SET NOT NULL,
    ADD CONSTRAINT fk_coordination_resource_tenant
        FOREIGN KEY (tenant_id) REFERENCES tenant_registry(tenant_id),
    ADD PRIMARY KEY (tenant_id, resource_key);
ALTER TABLE coordination_permit
    ALTER COLUMN tenant_id SET NOT NULL,
    ADD CONSTRAINT fk_coordination_permit_tenant
        FOREIGN KEY (tenant_id) REFERENCES tenant_registry(tenant_id),
    ADD CONSTRAINT fk_coordination_permit_resource_tenant
        FOREIGN KEY (tenant_id, resource_key)
        REFERENCES coordination_resource(tenant_id, resource_key);
CREATE UNIQUE INDEX uq_coordination_permit_active_holder
    ON coordination_permit(tenant_id, resource_key, holder_id)
    WHERE status = 'ACTIVE';
CREATE INDEX ix_coordination_permit_active_expiry
    ON coordination_permit(tenant_id, resource_key, expires_at, token)
    WHERE status = 'ACTIVE';

ALTER TABLE admission_rate_bucket ADD COLUMN tenant_id VARCHAR(63);
UPDATE admission_rate_bucket SET tenant_id = 'local';
ALTER TABLE admission_rate_bucket
    DROP CONSTRAINT admission_rate_bucket_pkey,
    ALTER COLUMN tenant_id SET NOT NULL,
    ADD CONSTRAINT fk_admission_rate_bucket_tenant
        FOREIGN KEY (tenant_id) REFERENCES tenant_registry(tenant_id),
    ADD PRIMARY KEY (tenant_id, bucket_key);
CREATE INDEX ix_admission_rate_bucket_tenant_updated
    ON admission_rate_bucket(tenant_id, updated_at DESC, bucket_key);
