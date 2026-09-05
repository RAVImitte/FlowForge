package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.RetryLifecycleObserver;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;

@Component
public class MicrometerRetryLifecycleObserver implements RetryLifecycleObserver {
    private final MeterRegistry meters;
    private final DistributionSummary backoff;

    public MicrometerRetryLifecycleObserver(MeterRegistry meters) {
        this.meters = meters;
        this.backoff = DistributionSummary.builder("flowforge.retries.backoff")
                .baseUnit("milliseconds")
                .description("Persisted task retry delay")
                .register(meters);
    }

    @Override
    public void scheduled(Duration delay) {
        afterCommit(() -> {
            meters.counter("flowforge.retries.scheduled").increment();
            backoff.record(delay.toMillis());
        });
    }

    @Override
    public void started() {
        afterCommit(() -> meters.counter("flowforge.retries.started").increment());
    }

    @Override
    public void exhausted() {
        afterCommit(() -> meters.counter("flowforge.retries.exhausted").increment());
    }

    @Override
    public void deadLettered() {
        afterCommit(() -> meters.counter("flowforge.tasks.dead.lettered").increment());
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
