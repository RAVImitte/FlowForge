package io.flowforge.application.schedule;

import java.time.Duration;

public interface ScheduleBackpressureObserver {
    default void pendingQueueSaturated(long depth, int limit) { }

    default void scheduleFiresThrottled(int requested, int granted, Duration retryAfter) { }
}
