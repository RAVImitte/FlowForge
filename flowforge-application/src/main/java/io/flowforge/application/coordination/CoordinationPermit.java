package io.flowforge.application.coordination;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record CoordinationPermit(
        TenantId tenantId,
        String resourceKey,
        String holderId,
        UUID token,
        Instant expiresAt
) {
    public CoordinationPermit {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        resourceKey = required(resourceKey, "resourceKey", 300);
        holderId = required(holderId, "holderId", 200);
        Objects.requireNonNull(token, "token must not be null");
        Objects.requireNonNull(expiresAt, "expiresAt must not be null");
    }

    private static String required(String value, String name, int maximumLength) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        String normalized = value.strip();
        if (normalized.length() > maximumLength) {
            throw new IllegalArgumentException(name + " must not exceed " + maximumLength + " characters");
        }
        return normalized;
    }
}
