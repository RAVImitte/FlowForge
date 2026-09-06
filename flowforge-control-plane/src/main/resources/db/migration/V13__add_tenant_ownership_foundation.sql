CREATE TABLE tenant_registry (
    tenant_id VARCHAR(63) PRIMARY KEY,
    display_name VARCHAR(200) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_tenant_registry_id
        CHECK (tenant_id ~ '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$'),
    CONSTRAINT ck_tenant_registry_status
        CHECK (status IN ('ACTIVE', 'SUSPENDED')),
    CONSTRAINT ck_tenant_registry_time_order
        CHECK (updated_at >= created_at)
);

INSERT INTO tenant_registry (tenant_id, display_name, status)
VALUES ('local', 'Local development tenant', 'ACTIVE');

-- The local default keeps rolling upgrades compatible while Slice 7.2b changes
-- every write path to provide explicit ownership. The default is removed there.
ALTER TABLE workflow_definition
    ADD COLUMN tenant_id VARCHAR(63) NOT NULL DEFAULT 'local'
        REFERENCES tenant_registry(tenant_id);

ALTER TABLE workflow_execution
    ADD COLUMN tenant_id VARCHAR(63) NOT NULL DEFAULT 'local'
        REFERENCES tenant_registry(tenant_id);

ALTER TABLE workflow_schedule
    ADD COLUMN tenant_id VARCHAR(63) NOT NULL DEFAULT 'local'
        REFERENCES tenant_registry(tenant_id);

CREATE INDEX ix_workflow_definition_tenant_created
    ON workflow_definition(tenant_id, created_at DESC, id);

CREATE INDEX ix_workflow_execution_tenant_created
    ON workflow_execution(tenant_id, created_at DESC, id);

CREATE INDEX ix_workflow_schedule_tenant_due
    ON workflow_schedule(tenant_id, next_fire_at, id)
    WHERE status = 'ACTIVE';
