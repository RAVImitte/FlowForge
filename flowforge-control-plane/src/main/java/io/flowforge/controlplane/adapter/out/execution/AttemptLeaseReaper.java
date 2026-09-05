package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.AttemptLeaseRecovery;
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
@ConditionalOnProperty(prefix = "flowforge.leases", name = "reaper-enabled", havingValue = "true")
public class AttemptLeaseReaper implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(AttemptLeaseReaper.class);

    private final AttemptLeaseRecovery recovery;
    private final Clock clock;
    private final MeterRegistry meters;
    private final int batchSize;
    private final AtomicBoolean reaping = new AtomicBoolean();

    public AttemptLeaseReaper(
            AttemptLeaseRecovery recovery,
            Clock clock,
            MeterRegistry meters,
            @Value("${flowforge.leases.batch-size:100}") int batchSize
    ) {
        if (batchSize < 1 || batchSize > 1_000) {
            throw new IllegalArgumentException("Lease reaper batch size must be between 1 and 1000");
        }
        this.recovery = recovery;
        this.clock = clock;
        this.meters = meters;
        this.batchSize = batchSize;
    }

    @Override
    public void run(ApplicationArguments args) {
        reapExpiredLeases();
    }

    @Scheduled(fixedDelayString = "${flowforge.leases.poll-interval-ms:250}")
    public void reapExpiredLeases() {
        if (!reaping.compareAndSet(false, true)) return;
        try {
            int reaped = recovery.reapExpiredLeases(batchSize, clock.instant());
            if (reaped > 0) {
                meters.counter("flowforge.leases.reaped").increment(reaped);
                LOGGER.debug("Reaped {} expired worker leases", reaped);
            }
        } catch (RuntimeException failure) {
            meters.counter("flowforge.leases.reaper.failures").increment();
            LOGGER.error("Worker-lease recovery failed", failure);
        } finally {
            reaping.set(false);
        }
    }
}
