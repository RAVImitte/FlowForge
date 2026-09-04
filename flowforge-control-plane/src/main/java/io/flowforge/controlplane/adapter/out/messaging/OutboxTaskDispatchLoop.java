package io.flowforge.controlplane.adapter.out.messaging;

import io.flowforge.application.execution.DurableTaskQueue;
import io.flowforge.controlplane.config.OutboxProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(prefix = "flowforge.outbox", name = "command-dispatch-enabled", havingValue = "true")
public class OutboxTaskDispatchLoop implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxTaskDispatchLoop.class);

    private final DurableTaskQueue queue;
    private final OutboxProperties properties;
    private final Clock clock;
    private final AtomicBoolean dispatching = new AtomicBoolean();

    public OutboxTaskDispatchLoop(
            DurableTaskQueue queue,
            OutboxProperties properties,
            Clock clock,
            @Value("${flowforge.execution.dispatch-enabled:true}") boolean inProcessDispatchEnabled
    ) {
        if (inProcessDispatchEnabled) {
            throw new IllegalStateException(
                    "Outbox command dispatch and in-process dispatch cannot be enabled together"
            );
        }
        this.queue = queue;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public void run(ApplicationArguments args) {
        dispatchReadyTasks();
    }

    @Scheduled(fixedDelayString = "${flowforge.outbox.command-dispatch-interval-ms:250}")
    public void dispatchReadyTasks() {
        if (!dispatching.compareAndSet(false, true)) return;
        try {
            int enqueued = queue.enqueueReadyTasks(properties.batchSize(), clock.instant());
            if (enqueued > 0) LOGGER.debug("Enqueued {} task commands", enqueued);
        } catch (RuntimeException failure) {
            LOGGER.error("Outbox task dispatch failed", failure);
        } finally {
            dispatching.set(false);
        }
    }
}
