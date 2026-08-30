CREATE TABLE workflow_definition (
    id UUID PRIMARY KEY,
    lifecycle_status VARCHAR(20) NOT NULL,
    lock_version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_workflow_lifecycle_status
        CHECK (lifecycle_status IN ('ACTIVE', 'ARCHIVED')),
    CONSTRAINT ck_workflow_lock_version
        CHECK (lock_version >= 0)
);

CREATE TABLE workflow_version (
    id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES workflow_definition(id),
    version_number INTEGER NOT NULL,
    version_status VARCHAR(20) NOT NULL,
    name VARCHAR(200) NOT NULL,
    description VARCHAR(2000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ,
    CONSTRAINT uq_workflow_version UNIQUE (workflow_id, version_number),
    CONSTRAINT ck_workflow_version_number CHECK (version_number > 0),
    CONSTRAINT ck_workflow_version_status
        CHECK (version_status IN ('DRAFT', 'PUBLISHED')),
    CONSTRAINT ck_workflow_publication
        CHECK ((version_status = 'DRAFT' AND published_at IS NULL)
            OR (version_status = 'PUBLISHED' AND published_at IS NOT NULL))
);

CREATE UNIQUE INDEX uq_workflow_single_draft
    ON workflow_version(workflow_id)
    WHERE version_status = 'DRAFT';

CREATE INDEX ix_workflow_version_lookup
    ON workflow_version(workflow_id, version_number DESC);

CREATE TABLE workflow_task (
    id UUID PRIMARY KEY,
    workflow_version_id UUID NOT NULL REFERENCES workflow_version(id) ON DELETE CASCADE,
    task_key VARCHAR(100) NOT NULL,
    task_name VARCHAR(200) NOT NULL,
    task_type VARCHAR(100) NOT NULL,
    configuration JSONB NOT NULL DEFAULT '{}'::jsonb,
    position INTEGER NOT NULL,
    CONSTRAINT uq_workflow_task_key UNIQUE (workflow_version_id, task_key),
    CONSTRAINT uq_workflow_task_position UNIQUE (workflow_version_id, position),
    CONSTRAINT ck_workflow_task_position CHECK (position >= 0),
    CONSTRAINT ck_workflow_task_configuration CHECK (jsonb_typeof(configuration) = 'object')
);

CREATE TABLE workflow_dependency (
    workflow_version_id UUID NOT NULL,
    task_key VARCHAR(100) NOT NULL,
    depends_on_task_key VARCHAR(100) NOT NULL,
    PRIMARY KEY (workflow_version_id, task_key, depends_on_task_key),
    CONSTRAINT fk_dependency_task
        FOREIGN KEY (workflow_version_id, task_key)
        REFERENCES workflow_task(workflow_version_id, task_key)
        ON DELETE CASCADE,
    CONSTRAINT fk_dependency_prerequisite
        FOREIGN KEY (workflow_version_id, depends_on_task_key)
        REFERENCES workflow_task(workflow_version_id, task_key)
        ON DELETE CASCADE,
    CONSTRAINT ck_workflow_dependency_not_self
        CHECK (task_key <> depends_on_task_key)
);

CREATE INDEX ix_workflow_dependency_prerequisite
    ON workflow_dependency(workflow_version_id, depends_on_task_key);
