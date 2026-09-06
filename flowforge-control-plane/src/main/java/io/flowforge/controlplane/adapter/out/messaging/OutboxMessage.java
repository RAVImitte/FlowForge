package io.flowforge.controlplane.adapter.out.messaging;

import io.flowforge.observability.TraceContextSnapshot;

import java.time.Instant;
import java.util.UUID;

public record OutboxMessage(
        String tenantId,
        UUID id,
        UUID workflowExecutionId,
        UUID taskExecutionId,
        String messageKind,
        String topic,
        String recordKey,
        String eventType,
        int schemaVersion,
        String payload,
        int attemptCount,
        Instant createdAt,
        UUID claimToken,
        TraceContextSnapshot traceContext
) {
}
