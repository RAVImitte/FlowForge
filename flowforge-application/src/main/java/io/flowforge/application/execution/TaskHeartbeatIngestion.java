package io.flowforge.application.execution;

import java.time.Instant;

public interface TaskHeartbeatIngestion {
    TaskHeartbeatIngestionOutcome ingest(InboundTaskHeartbeat heartbeat, String serializedEnvelope, Instant receivedAt);
}
