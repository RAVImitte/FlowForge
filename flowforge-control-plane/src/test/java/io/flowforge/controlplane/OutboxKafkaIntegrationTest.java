package io.flowforge.controlplane;

import io.flowforge.application.execution.DurableTaskQueue;
import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.controlplane.adapter.out.messaging.OutboxPublisher;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskCommandV1;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "flowforge.kafka.enabled=true",
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.outbox.publisher-enabled=true",
        "flowforge.outbox.command-dispatch-enabled=false",
        "flowforge.outbox.poll-interval-ms=3600000",
        "flowforge.outbox.instance-id=outbox-kafka-test"
})
@Testcontainers(disabledWithoutDocker = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OutboxKafkaIntegrationTest {
    private static final Instant TIME = Instant.parse("2026-09-04T12:00:00Z");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka:4.3.1")
    );

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
    WorkflowService workflowService;

    @Autowired
    ExecutionRepository executionRepository;

    @Autowired
    DurableTaskQueue durableTaskQueue;

    @Autowired
    OutboxPublisher publisher;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void publishesCommandsAndEventsWithStableKeysHeadersAndPayloads() throws Exception {
        WorkflowDefinition created = workflowService.create(new WorkflowDraft(
                "Kafka outbox test",
                null,
                List.of(new TaskDefinition("ROOT", "Root", "NOOP", Map.of())),
                List.of()
        ));
        WorkflowDefinition published = workflowService.publish(created.id(), created.lockVersion());
        var execution = executionRepository.start(published.id(), "outbox-kafka", TIME);
        UUID executionId = execution.workflow().id();
        assertThat(durableTaskQueue.enqueueReadyTasks(10, TIME.plusSeconds(1))).isEqualTo(1);

        try (KafkaConsumer<String, String> consumer = consumer()) {
            consumer.subscribe(List.of(
                    FlowForgeTopics.TASK_COMMANDS_V1,
                    FlowForgeTopics.EXECUTION_EVENTS_V1
            ));

            assertThat(publisher.publishAvailable()).isEqualTo(5);
            var records = poll(consumer, 5, Duration.ofSeconds(15));

            assertThat(records).hasSize(5);
            assertThat(records).filteredOn(record -> record.topic().equals(FlowForgeTopics.EXECUTION_EVENTS_V1))
                    .hasSize(4)
                    .allSatisfy(record -> assertThat(record.key()).isEqualTo(executionId.toString()));
            var commandRecord = records.stream()
                    .filter(record -> record.topic().equals(FlowForgeTopics.TASK_COMMANDS_V1))
                    .findFirst()
                    .orElseThrow();
            MessageEnvelope<TaskCommandV1> command = objectMapper.readValue(
                    commandRecord.value(),
                    new TypeReference<MessageEnvelope<TaskCommandV1>>() {
                    }
            );
            assertThat(commandRecord.key()).isEqualTo(command.payload().taskExecutionId().toString());
            assertThat(command.payload().workflowExecutionId()).isEqualTo(executionId);
            assertThat(command.payload().taskKey()).isEqualTo("ROOT");
            assertThat(header(commandRecord.headers().lastHeader("flowforge-event-id")))
                    .isEqualTo(command.eventId().toString());
            assertThat(header(commandRecord.headers().lastHeader("flowforge-correlation-id")))
                    .isEqualTo(executionId.toString());
        }

        long publishedRows = jdbc.sql("""
                SELECT COUNT(*)
                  FROM control_plane_outbox
                 WHERE workflow_execution_id = :executionId
                   AND status = 'PUBLISHED'
                """)
                .param("executionId", executionId)
                .query(Long.class)
                .single();
        assertThat(publishedRows).isEqualTo(5);
    }

    private static KafkaConsumer<String, String> consumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(properties);
    }

    private static List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> poll(
            KafkaConsumer<String, String> consumer,
            int expected,
            Duration timeout
    ) {
        List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> records = new ArrayList<>();
        Instant deadline = Instant.now().plus(timeout);
        while (records.size() < expected && Instant.now().isBefore(deadline)) {
            consumer.poll(Duration.ofMillis(250)).forEach(records::add);
        }
        return records;
    }

    private static String header(Header header) {
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
