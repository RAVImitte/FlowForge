package io.flowforge.worker.persistence;

import java.util.UUID;

public record WorkerResultMessage(
        UUID id,
        UUID workflowExecutionId,
        String topic,
        String recordKey,
        String eventType,
        int schemaVersion,
        String payload,
        int attemptCount,
        UUID claimToken
) {
}
