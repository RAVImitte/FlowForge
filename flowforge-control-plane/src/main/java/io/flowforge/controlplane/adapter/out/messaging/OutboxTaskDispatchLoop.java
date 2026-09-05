package io.flowforge.controlplane.adapter.out.messaging;

import io.flowforge.application.coordination.TokenBucketDecision;
import io.flowforge.application.coordination.TokenBucketPolicy;
import io.flowforge.application.coordination.TokenBucketRateLimiter;
import io.flowforge.application.execution.AdmissionBackpressureObserver;
import io.flowforge.application.execution.DurableTaskQueue;
import io.flowforge.application.execution.ReadyQueueSnapshot;
import io.flowforge.controlplane.config.OutboxProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(prefix = "flowforge.outbox", name = "command-dispatch-enabled", havingValue = "true")
public class OutboxTaskDispatchLoop implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxTaskDispatchLoop.class);

    private final DurableTaskQueue queue;
    private final OutboxProperties properties;
    private final Clock clock;
    private final TokenBucketRateLimiter rateLimiter;
    private final TokenBucketPolicy rateLimitPolicy;
    private final AdmissionBackpressureObserver backpressureObserver;
    private final AtomicBoolean dispatching = new AtomicBoolean();

    @Autowired
    public OutboxTaskDispatchLoop(
            DurableTaskQueue queue,
            OutboxProperties properties,
            Clock clock,
            @Value("${flowforge.execution.dispatch-enabled:true}") boolean inProcessDispatchEnabled,
            TokenBucketRateLimiter rateLimiter,
            AdmissionBackpressureObserver backpressureObserver,
            @Value("${flowforge.rate-limits.task-dispatch.capacity:200}") int capacity,
            @Value("${flowforge.rate-limits.task-dispatch.refill-tokens:200}") int refillTokens,
            @Value("${flowforge.rate-limits.task-dispatch.refill-period:1s}") Duration refillPeriod
    ) {
        if (inProcessDispatchEnabled) {
            throw new IllegalStateException(
                    "Outbox command dispatch and in-process dispatch cannot be enabled together"
            );
        }
        this.queue = queue;
        this.properties = properties;
        this.clock = clock;
        this.rateLimiter = rateLimiter;
        this.rateLimitPolicy = new TokenBucketPolicy(capacity, refillTokens, refillPeriod);
        this.backpressureObserver = backpressureObserver;
    }

    OutboxTaskDispatchLoop(
            DurableTaskQueue queue,
            OutboxProperties properties,
            Clock clock,
            boolean inProcessDispatchEnabled
    ) {
        this(
                queue, properties, clock, inProcessDispatchEnabled,
                (key, policy, requested, now) -> new TokenBucketDecision(requested, Duration.ZERO),
                new AdmissionBackpressureObserver() { },
                1_000, 1_000, Duration.ofSeconds(1)
        );
    }

    @Override
    public void run(ApplicationArguments args) {
        dispatchReadyTasks();
    }

    @Scheduled(fixedDelayString = "${flowforge.outbox.command-dispatch-interval-ms:250}")
    public void dispatchReadyTasks() {
        if (!dispatching.compareAndSet(false, true)) return;
        try {
            var now = clock.instant();
            ReadyQueueSnapshot snapshot = queue.readyQueue(now, properties.batchSize());
            if (snapshot == null) snapshot = ReadyQueueSnapshot.unknown(properties.batchSize());
            backpressureObserver.readyQueueObserved(snapshot.depth(), snapshot.oldestAge());
            int requested = (int) Math.min(properties.batchSize(), snapshot.depth());
            if (requested == 0) return;
            TokenBucketDecision decision = rateLimiter.consume(
                    "task-dispatch", rateLimitPolicy, requested, now
            );
            if (decision.throttled(requested)) {
                backpressureObserver.taskDispatchThrottled(
                        requested, decision.granted(), decision.retryAfter()
                );
            }
            if (decision.granted() == 0) return;
            int enqueued = queue.enqueueReadyTasks(decision.granted(), now);
            if (enqueued > 0) LOGGER.debug("Enqueued {} task commands", enqueued);
        } catch (RuntimeException failure) {
            LOGGER.error("Outbox task dispatch failed", failure);
        } finally {
            dispatching.set(false);
        }
    }
}
