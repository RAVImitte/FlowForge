package io.flowforge.application.execution;

import java.time.Duration;

public interface AdmissionBackpressureObserver {
    default void taskDispatchThrottled(int requested, int granted, Duration retryAfter) { }

    default void readyQueueRejected() { }

    default void readyQueueObserved(long depth, Duration oldestAge) { }
}
