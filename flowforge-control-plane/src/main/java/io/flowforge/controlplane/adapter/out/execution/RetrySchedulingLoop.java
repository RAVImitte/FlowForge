package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.DurableTaskQueue;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(
        prefix = "flowforge.retries",
        name = "scheduler-enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class RetrySchedulingLoop implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetrySchedulingLoop.class);

    private final DurableTaskQueue queue;
    private final Clock clock;
    private final MeterRegistry meters;
    private final int batchSize;
    private final AtomicBoolean scheduling = new AtomicBoolean();

    public RetrySchedulingLoop(
            DurableTaskQueue queue,
            Clock clock,
            MeterRegistry meters,
            @Value("${flowforge.retries.batch-size:100}") int batchSize
    ) {
        if (batchSize < 1 || batchSize > 1_000) {
            throw new IllegalArgumentException("Retry scheduler batch size must be between 1 and 1000");
        }
        this.queue = queue;
        this.clock = clock;
        this.meters = meters;
        this.batchSize = batchSize;
    }

    @Override
    public void run(ApplicationArguments args) {
        releaseDueRetries();
    }

    @Scheduled(fixedDelayString = "${flowforge.retries.poll-interval-ms:250}")
    public void releaseDueRetries() {
        if (!scheduling.compareAndSet(false, true)) return;
        try {
            int released = queue.releaseDueRetries(batchSize, clock.instant());
            if (released > 0) {
                meters.counter("flowforge.retries.released").increment(released);
                LOGGER.debug("Released {} due task retries", released);
            }
        } catch (RuntimeException failure) {
            meters.counter("flowforge.retries.scheduler.failures").increment();
            LOGGER.error("Due-retry scheduling failed", failure);
        } finally {
            scheduling.set(false);
        }
    }
}
