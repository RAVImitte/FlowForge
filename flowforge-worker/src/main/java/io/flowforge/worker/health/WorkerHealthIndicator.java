package io.flowforge.worker.health;

import io.flowforge.worker.config.WorkerProperties;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("worker")
public class WorkerHealthIndicator implements HealthIndicator {
    private final WorkerProperties properties;

    public WorkerHealthIndicator(WorkerProperties properties) {
        this.properties = properties;
    }

    @Override
    public Health health() {
        return Health.up()
                .withDetail("workerId", properties.id())
                .withDetail("consumerGroup", properties.consumerGroup())
                .withDetail("mode", "kafka-foundation")
                .build();
    }
}
