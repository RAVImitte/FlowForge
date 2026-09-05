package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.AdmissionBackpressureObserver;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class MicrometerAdmissionBackpressureObserver implements AdmissionBackpressureObserver {
    private final MeterRegistry meters;

    public MicrometerAdmissionBackpressureObserver(MeterRegistry meters) {
        this.meters = meters;
    }

    @Override
    public void taskDispatchThrottled(int requested, int granted, Duration retryAfter) {
        meters.counter("flowforge.admission.throttled", "scope", "task-dispatch")
                .increment(requested - granted);
        meters.summary("flowforge.admission.retry.after", "scope", "task-dispatch")
                .record(retryAfter.toMillis());
    }

    @Override
    public void readyQueueRejected() {
        meters.counter("flowforge.admission.rejected", "reason", "ready-queue-saturated")
                .increment();
    }

    @Override
    public void readyQueueObserved(long depth, Duration oldestAge) {
        meters.summary("flowforge.admission.queue.depth", "scope", "task-ready").record(depth);
        meters.summary("flowforge.admission.queue.age", "scope", "task-ready")
                .record(oldestAge.toMillis());
    }
}
