package io.flowforge.application.audit;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record SecurityAuditEvent(
        UUID id,
        Instant occurredAt,
        TenantId tenantId,
        String actor,
        String action,
        String targetType,
        String targetId,
        AuditOutcome outcome,
        String httpMethod,
        String httpPath,
        Integer statusCode,
        UUID requestId
) {
    public SecurityAuditEvent {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        actor = require(actor, "actor", 200);
        action = require(action, "action", 100);
        targetType = require(targetType, "targetType", 100);
        targetId = normalize(targetId, 500);
        Objects.requireNonNull(outcome, "outcome must not be null");
        httpMethod = require(httpMethod, "httpMethod", 10);
        httpPath = require(httpPath, "httpPath", 1000);
        if (statusCode != null && (statusCode < 100 || statusCode > 599)) {
            throw new IllegalArgumentException("statusCode must be a valid HTTP status");
        }
        Objects.requireNonNull(requestId, "requestId must not be null");
    }

    private static String require(String value, String field, int max) {
        String normalized = normalize(value, max);
        if (normalized == null) throw new IllegalArgumentException(field + " must not be blank");
        return normalized;
    }

    private static String normalize(String value, int max) {
        if (value == null || value.isBlank()) return null;
        String normalized = value.strip();
        if (normalized.length() > max) throw new IllegalArgumentException("audit field exceeds " + max + " characters");
        return normalized;
    }
}
