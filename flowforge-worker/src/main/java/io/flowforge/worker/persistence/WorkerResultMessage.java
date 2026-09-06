package io.flowforge.worker.persistence;

import io.flowforge.observability.TraceContextSnapshot;

import java.util.UUID;

public record WorkerResultMessage(
        String tenantId,
        UUID id,
        UUID workflowExecutionId,
        String topic,
        String recordKey,
        String eventType,
        int schemaVersion,
        String payload,
        int attemptCount,
        UUID claimToken,
        TraceContextSnapshot traceContext
) {
}
