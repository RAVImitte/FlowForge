package io.flowforge.controlplane.config;

import io.flowforge.messaging.FlowForgeTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(FlowForgeKafkaProperties.class)
@ConditionalOnProperty(prefix = "flowforge.kafka", name = "enabled", havingValue = "true")
public class KafkaTopicConfiguration {
    @Bean
    NewTopic taskCommandsTopic(FlowForgeKafkaProperties properties) {
        return topic(FlowForgeTopics.TASK_COMMANDS_V1, properties);
    }

    @Bean
    NewTopic taskCommandsDeadLetterTopic(FlowForgeKafkaProperties properties) {
        return topic(FlowForgeTopics.TASK_COMMANDS_DLQ_V1, properties);
    }

    @Bean
    NewTopic taskResultsTopic(FlowForgeKafkaProperties properties) {
        return topic(FlowForgeTopics.TASK_RESULTS_V1, properties);
    }

    @Bean
    NewTopic taskResultsDeadLetterTopic(FlowForgeKafkaProperties properties) {
        return topic(FlowForgeTopics.TASK_RESULTS_DLQ_V1, properties);
    }

    @Bean
    NewTopic taskHeartbeatsTopic(FlowForgeKafkaProperties properties) {
        return topic(FlowForgeTopics.TASK_HEARTBEATS_V1, properties);
    }

    @Bean
    NewTopic taskHeartbeatsDeadLetterTopic(FlowForgeKafkaProperties properties) {
        return topic(FlowForgeTopics.TASK_HEARTBEATS_DLQ_V1, properties);
    }

    @Bean
    NewTopic executionEventsTopic(FlowForgeKafkaProperties properties) {
        return topic(FlowForgeTopics.EXECUTION_EVENTS_V1, properties);
    }

    private static NewTopic topic(String name, FlowForgeKafkaProperties properties) {
        return TopicBuilder.name(name)
                .partitions(properties.partitions())
                .replicas(properties.replicationFactor())
                .build();
    }
}
