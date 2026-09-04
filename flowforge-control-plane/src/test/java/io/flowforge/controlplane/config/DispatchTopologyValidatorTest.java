package io.flowforge.controlplane.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DispatchTopologyValidatorTest {
    @Test
    void acceptsPhaseTwoAndDistributedTopologies() {
        assertThatCode(() -> validate(false, true, false, false, false, false))
                .doesNotThrowAnyException();
        assertThatCode(() -> validate(true, false, true, true, true, true))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsCompetingDispatchers() {
        assertThatThrownBy(() -> validate(true, true, true, true, true, true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot be enabled together");
    }

    @Test
    void rejectsKafkaComponentsWithoutKafka() {
        assertThatThrownBy(() -> validate(false, false, false, true, false, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("require flowforge.kafka.enabled=true");
    }

    @Test
    void rejectsIncompleteDistributedExecutionTopology() {
        assertThatThrownBy(() -> validate(true, false, true, false, true, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires the outbox publisher");
        assertThatThrownBy(() -> validate(true, false, true, true, false, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires the task-result consumer");
    }

    @Test
    void rejectsProductionTopologyWithoutAConsumerOfReadyWork() {
        assertThatThrownBy(() -> validate(true, false, false, true, true, true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("active task dispatcher");
    }

    private static void validate(
            boolean kafkaEnabled,
            boolean inProcessDispatchEnabled,
            boolean commandDispatchEnabled,
            boolean outboxPublisherEnabled,
            boolean resultConsumerEnabled,
            boolean activeDispatcherRequired
    ) throws Exception {
        new DispatchTopologyValidator(
                kafkaEnabled,
                inProcessDispatchEnabled,
                commandDispatchEnabled,
                outboxPublisherEnabled,
                resultConsumerEnabled,
                activeDispatcherRequired
        ).afterPropertiesSet();
    }
}
