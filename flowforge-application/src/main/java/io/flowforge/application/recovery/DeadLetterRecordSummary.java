package io.flowforge.application.recovery;

import java.time.Instant;

public record DeadLetterRecordSummary(
        String deadLetterTopic,
        int partition,
        long offset,
        String sourceTopic,
        String recordId,
        String tenantId,
        String key,
        String eventId,
        String correlationId,
        String eventType,
        String schemaVersion,
        String failureClass,
        Instant failedAt,
        int payloadBytes,
        String payloadSha256
) {
}
