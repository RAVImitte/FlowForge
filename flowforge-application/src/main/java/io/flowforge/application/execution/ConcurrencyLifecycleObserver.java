package io.flowforge.application.execution;

public interface ConcurrencyLifecycleObserver {
    void acquired(String scope);

    void released(String scope);

    void workflowRejected();

    void taskDeferred();
}
