package io.flowforge.worker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("flowforge.worker")
public record WorkerProperties(String id, String consumerGroup, Duration kafkaHealthTimeout) {
    public WorkerProperties {
        if (id == null || id.isBlank()) throw new IllegalArgumentException("Worker id must not be blank");
        if (consumerGroup == null || consumerGroup.isBlank()) {
            throw new IllegalArgumentException("Worker consumer group must not be blank");
        }
        if (kafkaHealthTimeout == null || kafkaHealthTimeout.isZero() || kafkaHealthTimeout.isNegative()) {
            throw new IllegalArgumentException("Kafka health timeout must be positive");
        }
        id = id.trim();
        consumerGroup = consumerGroup.trim();
    }
}
