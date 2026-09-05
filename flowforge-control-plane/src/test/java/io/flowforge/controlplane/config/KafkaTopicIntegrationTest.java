package io.flowforge.controlplane.config;

import io.flowforge.messaging.FlowForgeTopics;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
        "flowforge.kafka.enabled=true",
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false"
})
class KafkaTopicIntegrationTest {
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @DynamicPropertySource
    static void infrastructureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Test
    void provisionsVersionedTopicsOnKafka() throws Exception {
        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Set<String> names = client.listTopics().names().get(Duration.ofSeconds(10).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            assertThat(names).contains(
                    FlowForgeTopics.TASK_COMMANDS_V1,
                    FlowForgeTopics.TASK_COMMANDS_DLQ_V1,
                    FlowForgeTopics.TASK_RESULTS_V1,
                    FlowForgeTopics.TASK_RESULTS_DLQ_V1,
                    FlowForgeTopics.TASK_HEARTBEATS_V1,
                    FlowForgeTopics.TASK_HEARTBEATS_DLQ_V1,
                    FlowForgeTopics.EXECUTION_EVENTS_V1
            );
            assertThat(client.describeTopics(List.of(FlowForgeTopics.TASK_COMMANDS_V1))
                    .allTopicNames()
                    .get(Duration.ofSeconds(10).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
                    .get(FlowForgeTopics.TASK_COMMANDS_V1)
                    .partitions()).hasSize(6);
        }
    }
}
