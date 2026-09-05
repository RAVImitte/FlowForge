ALTER TABLE workflow_version
    ADD COLUMN max_concurrent_executions INTEGER,
    ADD CONSTRAINT ck_workflow_version_max_concurrency
        CHECK (max_concurrent_executions BETWEEN 1 AND 100000);

ALTER TABLE workflow_task
    ADD COLUMN max_concurrency INTEGER,
    ADD CONSTRAINT ck_workflow_task_max_concurrency
        CHECK (max_concurrency BETWEEN 1 AND 100000);

ALTER TABLE workflow_execution
    ADD COLUMN concurrency_permit_token UUID REFERENCES coordination_permit(token),
    ADD CONSTRAINT uq_workflow_execution_concurrency_permit UNIQUE (concurrency_permit_token);

ALTER TABLE task_attempt
    ADD COLUMN concurrency_permit_token UUID REFERENCES coordination_permit(token),
    ADD CONSTRAINT uq_task_attempt_concurrency_permit UNIQUE (concurrency_permit_token);

CREATE INDEX ix_workflow_execution_concurrency
    ON workflow_execution(workflow_version_id, status)
    WHERE status IN ('PENDING', 'RUNNING', 'CANCELLING');

CREATE INDEX ix_task_execution_concurrency
    ON task_execution(workflow_execution_id, task_key)
    WHERE status = 'RUNNING';
