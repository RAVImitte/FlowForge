package io.flowforge.controlplane.config;

import io.flowforge.messaging.FlowForgeTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaTopicConfigurationTest {
    private final KafkaTopicConfiguration configuration = new KafkaTopicConfiguration();

    @Test
    void definesEveryVersionedTopicWithExplicitCapacity() {
        FlowForgeKafkaProperties properties = new FlowForgeKafkaProperties(6, 1);

        List<NewTopic> topics = List.of(
                configuration.taskCommandsTopic(properties),
                configuration.taskCommandsDeadLetterTopic(properties),
                configuration.taskResultsTopic(properties),
                configuration.taskResultsDeadLetterTopic(properties),
                configuration.taskHeartbeatsTopic(properties),
                configuration.taskHeartbeatsDeadLetterTopic(properties),
                configuration.executionEventsTopic(properties)
        );

        assertThat(topics).extracting(NewTopic::name).containsExactly(
                FlowForgeTopics.TASK_COMMANDS_V1,
                FlowForgeTopics.TASK_COMMANDS_DLQ_V1,
                FlowForgeTopics.TASK_RESULTS_V1,
                FlowForgeTopics.TASK_RESULTS_DLQ_V1,
                FlowForgeTopics.TASK_HEARTBEATS_V1,
                FlowForgeTopics.TASK_HEARTBEATS_DLQ_V1,
                FlowForgeTopics.EXECUTION_EVENTS_V1
        );
        assertThat(topics).allSatisfy(topic -> {
            assertThat(topic.numPartitions()).isEqualTo(6);
            assertThat(topic.replicationFactor()).isEqualTo((short) 1);
        });
    }

    @Test
    void rejectsInvalidTopicCapacity() {
        assertThatThrownBy(() -> new FlowForgeKafkaProperties(0, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FlowForgeKafkaProperties(1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
