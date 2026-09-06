ALTER TABLE workflow_definition
    ADD CONSTRAINT uq_workflow_definition_tenant_identity
        UNIQUE (tenant_id, id);

ALTER TABLE workflow_execution
    DROP CONSTRAINT fk_execution_workflow,
    DROP CONSTRAINT uq_execution_idempotency,
    ADD CONSTRAINT fk_execution_workflow_tenant
        FOREIGN KEY (tenant_id, workflow_id)
        REFERENCES workflow_definition(tenant_id, id),
    ADD CONSTRAINT uq_execution_tenant_idempotency
        UNIQUE (tenant_id, workflow_id, idempotency_key);
