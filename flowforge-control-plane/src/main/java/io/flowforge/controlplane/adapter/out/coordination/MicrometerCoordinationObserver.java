package io.flowforge.controlplane.adapter.out.coordination;

import io.flowforge.application.coordination.CoordinationObserver;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class MicrometerCoordinationObserver implements CoordinationObserver {
    private final MeterRegistry meters;

    public MicrometerCoordinationObserver(MeterRegistry meters) {
        this.meters = meters;
    }

    @Override
    public void degraded(String operation) {
        meters.counter("flowforge.coordination.redis.failures", "operation", operation).increment();
    }

    @Override
    public void reconciled(int permitCount) {
        meters.counter("flowforge.coordination.reconciliations").increment();
        meters.summary("flowforge.coordination.reconciled.permits").record(permitCount);
    }
}
