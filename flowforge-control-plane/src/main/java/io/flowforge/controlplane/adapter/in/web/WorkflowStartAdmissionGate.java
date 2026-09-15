package io.flowforge.controlplane.adapter.in.web;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Component
final class WorkflowStartAdmissionGate {
    private final Semaphore permits;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final MeterRegistry meters;
    private final int limit;
    private final Duration retryAfter;

    WorkflowStartAdmissionGate(
            MeterRegistry meters,
            @Value("${flowforge.admission.workflow-start.max-in-flight:8}") int limit,
            @Value("${flowforge.admission.workflow-start.retry-after:1s}") Duration retryAfter
    ) {
        if (limit < 1) {
            throw new IllegalArgumentException("Workflow-start in-flight limit must be positive");
        }
        if (retryAfter == null || retryAfter.isZero() || retryAfter.isNegative()) {
            throw new IllegalArgumentException("Workflow-start retry delay must be positive");
        }
        this.meters = meters;
        this.limit = limit;
        this.retryAfter = retryAfter;
        this.permits = new Semaphore(limit, true);
        Gauge.builder("flowforge.admission.in.flight", inFlight, AtomicInteger::get)
                .tag("scope", "workflow-start")
                .description("Workflow-start requests currently admitted by this control-plane instance")
                .register(meters);
    }

    Lease acquire() {
        if (!permits.tryAcquire()) {
            meters.counter("flowforge.admission.rejected", "reason", "workflow-start-saturated").increment();
            throw new WorkflowStartOverloadedException(limit, retryAfter);
        }
        inFlight.incrementAndGet();
        return new Lease(permits, inFlight);
    }

    static final class Lease implements AutoCloseable {
        private final Semaphore permits;
        private final AtomicInteger inFlight;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Lease(Semaphore permits, AtomicInteger inFlight) {
            this.permits = permits;
            this.inFlight = inFlight;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                inFlight.decrementAndGet();
                permits.release();
            }
        }
    }
}
