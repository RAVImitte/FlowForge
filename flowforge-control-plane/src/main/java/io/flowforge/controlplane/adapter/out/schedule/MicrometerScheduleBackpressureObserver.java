package io.flowforge.controlplane.adapter.out.schedule;

import io.flowforge.application.schedule.ScheduleBackpressureObserver;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class MicrometerScheduleBackpressureObserver implements ScheduleBackpressureObserver {
    private final MeterRegistry meters;

    public MicrometerScheduleBackpressureObserver(MeterRegistry meters) {
        this.meters = meters;
    }

    @Override
    public void pendingQueueSaturated(long depth, int limit) {
        meters.counter("flowforge.admission.saturated", "scope", "schedule-pending").increment();
        meters.summary("flowforge.admission.queue.depth", "scope", "schedule-pending").record(depth);
    }

    @Override
    public void scheduleFiresThrottled(int requested, int granted, Duration retryAfter) {
        meters.counter("flowforge.admission.throttled", "scope", "schedule-fire")
                .increment(requested - granted);
        meters.summary("flowforge.admission.retry.after", "scope", "schedule-fire")
                .record(retryAfter.toMillis());
    }
}
