package io.flowforge.controlplane.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("flowforge.kafka.topics")
public record FlowForgeKafkaProperties(int partitions, int replicationFactor) {
    public FlowForgeKafkaProperties {
        if (partitions < 1) throw new IllegalArgumentException("Kafka topic partitions must be positive");
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("Kafka topic replication factor must be positive");
        }
    }
}
