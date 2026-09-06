package io.flowforge.application.recovery;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public record DeadLetterRecord(
        DeadLetterLocation location,
        String sourceTopic,
        String recordId,
        TenantId tenantId,
        String key,
        String value,
        Map<String, String> replayHeaders,
        String failureClass,
        Instant failedAt
) {
    public DeadLetterRecord {
        Objects.requireNonNull(location, "location must not be null");
        sourceTopic = requireText(sourceTopic, "sourceTopic");
        recordId = requireText(recordId, "recordId");
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(value, "value must not be null");
        replayHeaders = Map.copyOf(Objects.requireNonNull(replayHeaders, "replayHeaders must not be null"));
        failureClass = requireText(failureClass, "failureClass");
        Objects.requireNonNull(failedAt, "failedAt must not be null");
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
        return value;
    }
}
