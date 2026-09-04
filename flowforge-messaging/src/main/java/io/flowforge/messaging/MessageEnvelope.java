package io.flowforge.messaging;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record MessageEnvelope<T>(
        UUID eventId,
        String eventType,
        int schemaVersion,
        Instant occurredAt,
        UUID correlationId,
        T payload
) {
    public MessageEnvelope {
        Objects.requireNonNull(eventId, "eventId must not be null");
        eventType = ContractValidation.requireNonBlank(eventType, "eventType");
        if (schemaVersion < 1) throw new IllegalArgumentException("schemaVersion must be positive");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        Objects.requireNonNull(correlationId, "correlationId must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
    }
}
