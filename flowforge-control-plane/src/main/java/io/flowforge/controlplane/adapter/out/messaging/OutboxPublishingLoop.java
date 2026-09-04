package io.flowforge.controlplane.adapter.out.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "flowforge.outbox", name = "publisher-enabled", havingValue = "true")
public class OutboxPublishingLoop implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxPublishingLoop.class);

    private final OutboxPublisher publisher;

    public OutboxPublishingLoop(OutboxPublisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public void run(ApplicationArguments args) {
        publishAvailable();
    }

    @Scheduled(fixedDelayString = "${flowforge.outbox.poll-interval-ms:250}")
    public void publishAvailable() {
        try {
            int published = publisher.publishAvailable();
            if (published > 0) LOGGER.debug("Published {} outbox messages", published);
        } catch (RuntimeException failure) {
            LOGGER.error("Outbox publication failed", failure);
        }
    }
}
