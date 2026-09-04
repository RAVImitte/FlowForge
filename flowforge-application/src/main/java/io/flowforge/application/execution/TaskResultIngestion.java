package io.flowforge.application.execution;

import java.time.Instant;

public interface TaskResultIngestion {
    TaskResultIngestionOutcome ingest(InboundTaskResult result, String serializedEnvelope, Instant receivedAt);
}
