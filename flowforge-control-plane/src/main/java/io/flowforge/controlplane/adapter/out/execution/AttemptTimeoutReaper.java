package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.AttemptTimeoutRecovery;
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
        prefix = "flowforge.timeouts",
        name = "reaper-enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class AttemptTimeoutReaper implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(AttemptTimeoutReaper.class);

    private final AttemptTimeoutRecovery recovery;
    private final Clock clock;
    private final MeterRegistry meters;
    private final int batchSize;
    private final AtomicBoolean reaping = new AtomicBoolean();

    public AttemptTimeoutReaper(
            AttemptTimeoutRecovery recovery,
            Clock clock,
            MeterRegistry meters,
            @Value("${flowforge.timeouts.batch-size:100}") int batchSize
    ) {
        if (batchSize < 1 || batchSize > 1_000) {
            throw new IllegalArgumentException("Timeout reaper batch size must be between 1 and 1000");
        }
        this.recovery = recovery;
        this.clock = clock;
        this.meters = meters;
        this.batchSize = batchSize;
    }

    @Override
    public void run(ApplicationArguments args) {
        reapTimedOutAttempts();
    }

    @Scheduled(fixedDelayString = "${flowforge.timeouts.poll-interval-ms:250}")
    public void reapTimedOutAttempts() {
        if (!reaping.compareAndSet(false, true)) return;
        try {
            int reaped = recovery.reapTimedOutAttempts(batchSize, clock.instant());
            if (reaped > 0) {
                meters.counter("flowforge.timeouts.reaped").increment(reaped);
                LOGGER.debug("Reaped {} timed-out task attempts", reaped);
            }
        } catch (RuntimeException failure) {
            meters.counter("flowforge.timeouts.reaper.failures").increment();
            LOGGER.error("Attempt-timeout recovery failed", failure);
        } finally {
            reaping.set(false);
        }
    }
}
