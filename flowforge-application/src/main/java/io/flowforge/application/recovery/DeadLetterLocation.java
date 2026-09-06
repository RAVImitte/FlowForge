package io.flowforge.application.recovery;

public record DeadLetterLocation(String topic, int partition, long offset) {
    public DeadLetterLocation {
        if (topic == null || topic.isBlank()) throw new IllegalArgumentException("DLQ topic must not be blank");
        if (partition < 0) throw new IllegalArgumentException("DLQ partition must not be negative");
        if (offset < 0) throw new IllegalArgumentException("DLQ offset must not be negative");
    }

    public String identity() {
        return topic + ":" + partition + ":" + offset;
    }
}
