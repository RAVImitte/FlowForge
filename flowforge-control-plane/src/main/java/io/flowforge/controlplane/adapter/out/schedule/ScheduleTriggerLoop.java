package io.flowforge.controlplane.adapter.out.schedule;

import io.flowforge.application.schedule.ScheduleFireRunResult;
import io.flowforge.application.schedule.ScheduleFireService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(
        prefix = "flowforge.scheduling",
        name = "enabled",
        havingValue = "true"
)
public class ScheduleTriggerLoop implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduleTriggerLoop.class);

    private final ScheduleFireService service;
    private final MeterRegistry meters;
    private final ObservationRegistry observations;
    private final int materializationBatchSize;
    private final int processingBatchSize;
    private final AtomicBoolean running = new AtomicBoolean();

    public ScheduleTriggerLoop(
            ScheduleFireService service,
            MeterRegistry meters,
            ObservationRegistry observations,
            @Value("${flowforge.scheduling.materialization-batch-size:100}") int materializationBatchSize,
            @Value("${flowforge.scheduling.processing-batch-size:100}") int processingBatchSize
    ) {
        this.service = service;
        this.meters = meters;
        this.observations = observations;
        this.materializationBatchSize = validBatchSize(materializationBatchSize);
        this.processingBatchSize = validBatchSize(processingBatchSize);
    }

    @Override
    public void run(ApplicationArguments args) {
        processSchedules();
    }

    @Scheduled(fixedDelayString = "${flowforge.scheduling.poll-interval-ms:1000}")
    public void processSchedules() {
        if (!running.compareAndSet(false, true)) return;
        Observation observation = Observation.start("flowforge.schedule.process", observations);
        try (Observation.Scope ignored = observation.openScope()) {
            ScheduleFireRunResult result = service.runOnce(
                    materializationBatchSize,
                    processingBatchSize
            );
            increment("flowforge.schedules.materialized", result.materialized());
            increment("flowforge.schedules.misfires.skipped", result.skipped());
            increment("flowforge.schedules.triggers.claimed", result.claimed());
            increment("flowforge.schedules.triggers.started", result.started());
            increment("flowforge.schedules.triggers.failed", result.failed());
            increment("flowforge.schedules.triggers.released", result.released());
            increment("flowforge.schedules.triggers.stale.acknowledgements", result.staleAcknowledgements());
            increment("flowforge.schedules.triggers.throttled", result.throttled());
            increment("flowforge.schedules.materialization.capacity.deferred", result.capacityDeferred());
            meters.summary("flowforge.schedules.pending.depth").record(result.pendingDepth());
            meters.summary("flowforge.schedules.pending.oldest.age").record(result.oldestPendingAgeMillis());
            if (result.materialized() > 0 || result.claimed() > 0) {
                LOGGER.debug(
                        "Processed schedules: materialized={}, skipped={}, claimed={}, started={}, failed={}, released={}",
                        result.materialized(),
                        result.skipped(),
                        result.claimed(),
                        result.started(),
                        result.failed(),
                        result.released()
                );
            }
            observation.lowCardinalityKeyValue("flowforge.outcome", "success");
        } catch (RuntimeException failure) {
            observation.error(failure);
            observation.lowCardinalityKeyValue("flowforge.outcome", "error");
            meters.counter("flowforge.schedules.loop.failures").increment();
            LOGGER.error("Schedule trigger processing failed", failure);
        } finally {
            observation.stop();
            running.set(false);
        }
    }

    private void increment(String name, int amount) {
        if (amount > 0) meters.counter(name).increment(amount);
    }

    private static int validBatchSize(int value) {
        if (value < 1 || value > 1_000) {
            throw new IllegalArgumentException("Schedule batch size must be between 1 and 1000");
        }
        return value;
    }
}
