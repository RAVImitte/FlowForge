ALTER TABLE workflow_task
    ADD COLUMN secret_references jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE workflow_task
    ADD CONSTRAINT ck_workflow_task_secret_references_object
        CHECK (jsonb_typeof(secret_references) = 'object');

CREATE TABLE security_audit_event (
    id uuid PRIMARY KEY,
    occurred_at timestamptz NOT NULL,
    tenant_id varchar(100) NOT NULL REFERENCES tenant_registry(tenant_id),
    actor varchar(200) NOT NULL,
    action varchar(100) NOT NULL,
    target_type varchar(100) NOT NULL,
    target_id varchar(500),
    outcome varchar(20) NOT NULL,
    http_method varchar(10) NOT NULL,
    http_path varchar(1000) NOT NULL,
    status_code integer,
    request_id uuid NOT NULL,
    CONSTRAINT ck_security_audit_outcome
        CHECK (outcome IN ('ATTEMPTED', 'SUCCEEDED', 'DENIED', 'FAILED')),
    CONSTRAINT ck_security_audit_status
        CHECK (status_code IS NULL OR status_code BETWEEN 100 AND 599)
);

CREATE INDEX ix_security_audit_tenant_time
    ON security_audit_event(tenant_id, occurred_at DESC, id DESC);

CREATE INDEX ix_security_audit_request
    ON security_audit_event(request_id, occurred_at);

CREATE FUNCTION reject_security_audit_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'security audit events are append-only' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER security_audit_event_append_only
    BEFORE UPDATE OR DELETE ON security_audit_event
    FOR EACH ROW EXECUTE FUNCTION reject_security_audit_mutation();
