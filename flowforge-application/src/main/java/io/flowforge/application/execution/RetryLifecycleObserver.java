package io.flowforge.application.execution;

import java.time.Duration;

public interface RetryLifecycleObserver {
    void scheduled(Duration delay);

    void started();

    void exhausted();

    void deadLettered();
}
