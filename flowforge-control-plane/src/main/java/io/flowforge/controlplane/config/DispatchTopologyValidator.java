package io.flowforge.controlplane.config;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DispatchTopologyValidator implements InitializingBean {
    private final boolean kafkaEnabled;
    private final boolean inProcessDispatchEnabled;
    private final boolean commandDispatchEnabled;
    private final boolean outboxPublisherEnabled;
    private final boolean resultConsumerEnabled;
    private final boolean heartbeatConsumerEnabled;
    private final boolean activeDispatcherRequired;

    public DispatchTopologyValidator(
            @Value("${flowforge.kafka.enabled:false}") boolean kafkaEnabled,
            @Value("${flowforge.execution.dispatch-enabled:true}") boolean inProcessDispatchEnabled,
            @Value("${flowforge.outbox.command-dispatch-enabled:false}") boolean commandDispatchEnabled,
            @Value("${flowforge.outbox.publisher-enabled:false}") boolean outboxPublisherEnabled,
            @Value("${flowforge.results.consumer-enabled:false}") boolean resultConsumerEnabled,
            @Value("${flowforge.leases.heartbeat-consumer-enabled:false}") boolean heartbeatConsumerEnabled,
            @Value("${flowforge.execution.require-active-dispatcher:false}") boolean activeDispatcherRequired
    ) {
        this.kafkaEnabled = kafkaEnabled;
        this.inProcessDispatchEnabled = inProcessDispatchEnabled;
        this.commandDispatchEnabled = commandDispatchEnabled;
        this.outboxPublisherEnabled = outboxPublisherEnabled;
        this.resultConsumerEnabled = resultConsumerEnabled;
        this.heartbeatConsumerEnabled = heartbeatConsumerEnabled;
        this.activeDispatcherRequired = activeDispatcherRequired;
    }

    @Override
    public void afterPropertiesSet() {
        if (inProcessDispatchEnabled && commandDispatchEnabled) {
            throw new IllegalStateException(
                    "In-process dispatch and Kafka command dispatch cannot be enabled together"
            );
        }
        if (activeDispatcherRequired && !inProcessDispatchEnabled && !commandDispatchEnabled) {
            throw new IllegalStateException("An active task dispatcher is required");
        }
        if (!kafkaEnabled && (commandDispatchEnabled || outboxPublisherEnabled
                || resultConsumerEnabled || heartbeatConsumerEnabled)) {
            throw new IllegalStateException("Kafka-backed components require flowforge.kafka.enabled=true");
        }
        if (commandDispatchEnabled && !outboxPublisherEnabled) {
            throw new IllegalStateException("Kafka command dispatch requires the outbox publisher");
        }
        if (commandDispatchEnabled && !resultConsumerEnabled) {
            throw new IllegalStateException("Kafka command dispatch requires the task-result consumer");
        }
        if (commandDispatchEnabled && !heartbeatConsumerEnabled) {
            throw new IllegalStateException("Kafka command dispatch requires the task-heartbeat consumer");
        }
    }
}
