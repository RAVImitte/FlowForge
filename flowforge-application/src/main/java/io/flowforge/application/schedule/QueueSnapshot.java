package io.flowforge.application.schedule;

import java.time.Duration;
import java.util.Objects;

public record QueueSnapshot(long depth, Duration oldestAge) {
    public QueueSnapshot {
        if (depth < 0) throw new IllegalArgumentException("depth must not be negative");
        Objects.requireNonNull(oldestAge, "oldestAge must not be null");
        if (oldestAge.isNegative()) throw new IllegalArgumentException("oldestAge must not be negative");
    }

    public static QueueSnapshot empty() {
        return new QueueSnapshot(0, Duration.ZERO);
    }
}
