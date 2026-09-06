ALTER TABLE worker_result_outbox
    ADD COLUMN trace_parent VARCHAR(512),
    ADD COLUMN trace_state VARCHAR(512),
    ADD COLUMN trace_baggage VARCHAR(8192);

ALTER TABLE worker_result_outbox
    ADD CONSTRAINT ck_worker_result_outbox_trace_context CHECK (
        trace_parent IS NULL OR trace_parent !~ '[[:cntrl:]]'
    ),
    ADD CONSTRAINT ck_worker_result_outbox_trace_state CHECK (
        trace_state IS NULL OR trace_state !~ '[[:cntrl:]]'
    ),
    ADD CONSTRAINT ck_worker_result_outbox_trace_baggage CHECK (
        trace_baggage IS NULL OR trace_baggage !~ '[[:cntrl:]]'
    );
