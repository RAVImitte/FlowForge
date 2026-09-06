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
        String tenantId,
        T payload
) {
    public MessageEnvelope {
        Objects.requireNonNull(eventId, "eventId must not be null");
        eventType = ContractValidation.requireNonBlank(eventType, "eventType");
        if (schemaVersion < 1) throw new IllegalArgumentException("schemaVersion must be positive");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        Objects.requireNonNull(correlationId, "correlationId must not be null");
        tenantId = tenantId == null ? "local" : ContractValidation.requireTenantId(tenantId);
        Objects.requireNonNull(payload, "payload must not be null");
    }

    public MessageEnvelope(
            UUID eventId,
            String eventType,
            int schemaVersion,
            Instant occurredAt,
            UUID correlationId,
            T payload
    ) {
        this(eventId, eventType, schemaVersion, occurredAt, correlationId, "local", payload);
    }
}
