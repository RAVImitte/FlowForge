package io.flowforge.application.execution;

import java.time.Duration;
import java.util.Objects;

public record ReadyQueueSnapshot(long depth, Duration oldestAge) {
    public ReadyQueueSnapshot {
        if (depth < 0) throw new IllegalArgumentException("depth must not be negative");
        Objects.requireNonNull(oldestAge, "oldestAge must not be null");
        if (oldestAge.isNegative()) throw new IllegalArgumentException("oldestAge must not be negative");
    }

    public static ReadyQueueSnapshot unknown(int requestedLimit) {
        return new ReadyQueueSnapshot(requestedLimit, Duration.ZERO);
    }
}
