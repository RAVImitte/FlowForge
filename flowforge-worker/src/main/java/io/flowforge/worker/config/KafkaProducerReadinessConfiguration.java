package io.flowforge.worker.config;

import io.flowforge.kafka.KafkaProducerWarmup;
import io.flowforge.messaging.FlowForgeTopics;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "flowforge.kafka", name = "enabled", havingValue = "true")
public class KafkaProducerReadinessConfiguration {
    @Bean
    ApplicationRunner workerKafkaProducerWarmup(KafkaTemplate<String, String> kafka) {
        KafkaProducerWarmup warmup = new KafkaProducerWarmup(kafka);
        return ignored -> warmup.warm(List.of(
                FlowForgeTopics.TASK_RESULTS_V1,
                FlowForgeTopics.TASK_HEARTBEATS_V1,
                FlowForgeTopics.TASK_COMMANDS_DLQ_V1
        ));
    }
}
