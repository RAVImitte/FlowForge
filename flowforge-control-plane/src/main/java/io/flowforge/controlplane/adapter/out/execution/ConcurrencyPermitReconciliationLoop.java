package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.ConcurrencyPermitRecovery;
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
@ConditionalOnProperty(prefix = "flowforge.concurrency", name = "reconciliation-enabled", havingValue = "true")
public class ConcurrencyPermitReconciliationLoop implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrencyPermitReconciliationLoop.class);

    private final ConcurrencyPermitRecovery recovery;
    private final Clock clock;
    private final MeterRegistry meters;
    private final int batchSize;
    private final AtomicBoolean reconciling = new AtomicBoolean();

    public ConcurrencyPermitReconciliationLoop(
            ConcurrencyPermitRecovery recovery,
            Clock clock,
            MeterRegistry meters,
            @Value("${flowforge.concurrency.reconciliation-batch-size:200}") int batchSize
    ) {
        if (batchSize < 1 || batchSize > 1_000) {
            throw new IllegalArgumentException("Concurrency reconciliation batch size must be between 1 and 1000");
        }
        this.recovery = recovery;
        this.clock = clock;
        this.meters = meters;
        this.batchSize = batchSize;
    }

    @Override
    public void run(ApplicationArguments args) {
        reconcile();
    }

    @Scheduled(fixedDelayString = "${flowforge.concurrency.reconciliation-interval-ms:10000}")
    public void reconcile() {
        if (!reconciling.compareAndSet(false, true)) return;
        try {
            int reconciled = recovery.reconcileConcurrencyPermits(batchSize, clock.instant());
            if (reconciled > 0) {
                meters.counter("flowforge.concurrency.permits.reconciled").increment(reconciled);
                LOGGER.debug("Reconciled {} concurrency permits", reconciled);
            }
        } catch (RuntimeException failure) {
            meters.counter("flowforge.concurrency.reconciliation.failures").increment();
            LOGGER.error("Concurrency-permit reconciliation failed", failure);
        } finally {
            reconciling.set(false);
        }
    }
}
