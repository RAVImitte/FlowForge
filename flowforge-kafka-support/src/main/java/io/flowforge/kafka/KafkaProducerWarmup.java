package io.flowforge.kafka;

import org.apache.kafka.common.PartitionInfo;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

public final class KafkaProducerWarmup {
    private final KafkaTemplate<String, String> kafka;

    public KafkaProducerWarmup(KafkaTemplate<String, String> kafka) {
        this.kafka = Objects.requireNonNull(kafka, "kafka");
    }

    public void warm(Collection<String> topics) {
        if (topics == null || topics.isEmpty()) throw new IllegalArgumentException("Warm-up topics are required");
        for (String topic : topics) {
            if (topic == null || topic.isBlank()) throw new IllegalArgumentException("Warm-up topic must not be blank");
            List<PartitionInfo> partitions = kafka.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                throw new IllegalStateException("Kafka returned no partition metadata for " + topic);
            }
        }
    }
}
