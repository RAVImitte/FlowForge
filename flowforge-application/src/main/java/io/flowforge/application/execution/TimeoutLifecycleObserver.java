package io.flowforge.application.execution;

public interface TimeoutLifecycleObserver {
    void attemptTimedOut(boolean retryScheduled, boolean workflowFailed);
}
