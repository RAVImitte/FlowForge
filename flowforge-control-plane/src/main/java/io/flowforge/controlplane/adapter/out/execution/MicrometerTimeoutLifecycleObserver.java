package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.TimeoutLifecycleObserver;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Component
public class MicrometerTimeoutLifecycleObserver implements TimeoutLifecycleObserver {
    private final MeterRegistry meters;

    public MicrometerTimeoutLifecycleObserver(MeterRegistry meters) {
        this.meters = meters;
    }

    @Override
    public void attemptTimedOut(boolean retryScheduled, boolean workflowFailed) {
        afterCommit(() -> {
            meters.counter("flowforge.timeouts.tasks").increment();
            meters.counter(retryScheduled
                    ? "flowforge.timeouts.retried"
                    : "flowforge.timeouts.terminal").increment();
            if (workflowFailed) {
                meters.counter("flowforge.timeouts.workflows.failed").increment();
            }
        });
    }

    private static void afterCommit(Runnable action) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            action.run();
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                action.run();
            }
        });
    }
}
