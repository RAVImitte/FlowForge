package io.flowforge.controlplane.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("flowforge.outbox")
public record OutboxProperties(
        int batchSize,
        Duration leaseDuration,
        Duration publishTimeout,
        String instanceId
) {
    public OutboxProperties {
        if (batchSize < 1 || batchSize > 1_000) {
            throw new IllegalArgumentException("Outbox batch size must be between 1 and 1000");
        }
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("Outbox lease duration must be positive");
        }
        if (publishTimeout == null || publishTimeout.isZero() || publishTimeout.isNegative()) {
            throw new IllegalArgumentException("Outbox publish timeout must be positive");
        }
        if (instanceId == null || instanceId.isBlank()) {
            throw new IllegalArgumentException("Outbox instance id must not be blank");
        }
        instanceId = instanceId.trim();
    }
}
