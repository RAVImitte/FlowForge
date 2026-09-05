package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.ConcurrencyLifecycleObserver;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class MicrometerConcurrencyLifecycleObserver implements ConcurrencyLifecycleObserver {
    private final MeterRegistry meters;

    public MicrometerConcurrencyLifecycleObserver(MeterRegistry meters) {
        this.meters = meters;
    }

    @Override
    public void acquired(String scope) {
        meters.counter("flowforge.concurrency.permits.acquired", "scope", scope).increment();
    }

    @Override
    public void released(String scope) {
        meters.counter("flowforge.concurrency.permits.released", "scope", scope).increment();
    }

    @Override
    public void workflowRejected() {
        meters.counter("flowforge.concurrency.workflow.rejected").increment();
    }

    @Override
    public void taskDeferred() {
        meters.counter("flowforge.concurrency.task.deferred").increment();
    }
}
