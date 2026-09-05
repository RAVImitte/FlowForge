ALTER TABLE workflow_schedule
    DROP CONSTRAINT ck_workflow_schedule_status,
    ALTER COLUMN next_fire_at DROP NOT NULL;

ALTER TABLE workflow_schedule
    ADD CONSTRAINT ck_workflow_schedule_status
        CHECK (status IN ('ACTIVE', 'PAUSED', 'COMPLETED', 'DELETED')),
    ADD CONSTRAINT ck_workflow_schedule_next_fire CHECK (
        (status = 'COMPLETED' AND next_fire_at IS NULL)
        OR (status <> 'COMPLETED' AND next_fire_at IS NOT NULL)
    );

ALTER TABLE workflow_schedule_trigger
    ADD COLUMN workflow_id UUID,
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN available_at TIMESTAMPTZ,
    ADD COLUMN claimed_by VARCHAR(200),
    ADD COLUMN claimed_at TIMESTAMPTZ,
    ADD COLUMN claimed_until TIMESTAMPTZ,
    ADD COLUMN claim_token UUID;

UPDATE workflow_schedule_trigger target
   SET workflow_id = schedule.workflow_id,
       available_at = target.created_at
  FROM workflow_schedule schedule
 WHERE schedule.id = target.schedule_id;

ALTER TABLE workflow_schedule_trigger
    ALTER COLUMN workflow_id SET NOT NULL,
    ALTER COLUMN available_at SET NOT NULL,
    ADD CONSTRAINT fk_workflow_schedule_trigger_workflow
        FOREIGN KEY (workflow_id) REFERENCES workflow_definition(id),
    DROP CONSTRAINT ck_workflow_schedule_trigger_status,
    DROP CONSTRAINT ck_workflow_schedule_trigger_processed;

ALTER TABLE workflow_schedule_trigger
    ADD CONSTRAINT ck_workflow_schedule_trigger_status
        CHECK (status IN ('PENDING', 'PROCESSING', 'STARTED', 'SKIPPED', 'FAILED')),
    ADD CONSTRAINT ck_workflow_schedule_trigger_processed CHECK (
        (status IN ('PENDING', 'PROCESSING') AND processed_at IS NULL)
        OR (status IN ('STARTED', 'SKIPPED', 'FAILED') AND processed_at IS NOT NULL)
    ),
    ADD CONSTRAINT ck_workflow_schedule_trigger_claim CHECK (
        (status = 'PROCESSING' AND claimed_by IS NOT NULL AND claimed_at IS NOT NULL
            AND claimed_until IS NOT NULL AND claim_token IS NOT NULL)
        OR (status <> 'PROCESSING' AND claimed_by IS NULL AND claimed_at IS NULL
            AND claimed_until IS NULL AND claim_token IS NULL)
    ),
    ADD CONSTRAINT ck_workflow_schedule_trigger_attempt_count CHECK (attempt_count >= 0);

CREATE INDEX ix_workflow_schedule_trigger_pending
    ON workflow_schedule_trigger(available_at, scheduled_fire_at, id)
    WHERE status IN ('PENDING', 'PROCESSING');
