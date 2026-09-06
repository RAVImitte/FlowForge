package io.flowforge.controlplane;

import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.application.execution.DurableTaskQueue;
import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.domain.execution.TaskRunStatus;
import io.flowforge.domain.execution.WorkflowRunStatus;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.TaskDependency;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskResultOutcomeV1;
import io.flowforge.messaging.TaskResultV1;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "flowforge.kafka.enabled=true",
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.outbox.publisher-enabled=false",
        "flowforge.outbox.command-dispatch-enabled=false",
        "flowforge.results.consumer-enabled=true",
        "flowforge.leases.heartbeat-consumer-enabled=true",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false",
        "flowforge.results.consumer-group=result-kafka-integration-test",
        "flowforge.leases.heartbeat-consumer-group=heartbeat-dlq-integration-test",
        "flowforge.kafka.recovery.max-retries=1",
        "flowforge.kafka.recovery.initial-backoff=10ms",
        "flowforge.kafka.recovery.max-backoff=10ms"
})
@Testcontainers(disabledWithoutDocker = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TaskResultKafkaIntegrationTest {
    private static final Instant TIME = Instant.parse("2026-09-04T12:00:00Z");

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
    WorkflowService workflows;

    @Autowired
    ExecutionRepository executions;

    @Autowired
    DurableTaskQueue taskQueue;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    KafkaListenerEndpointRegistry listeners;

    @Autowired
    MeterRegistry meters;

    @Test
    void completesFanOutAndFanInThroughKafkaAndConsumesDuplicateResultsOnce() throws Exception {
        WorkflowDefinition created = workflows.create(TenantId.LOCAL, new WorkflowDraft(
                "Kafka result ingestion",
                null,
                List.of(task("ROOT"), task("LEFT"), task("RIGHT"), task("JOIN")),
                List.of(
                        new TaskDependency("LEFT", "ROOT"),
                        new TaskDependency("RIGHT", "ROOT"),
                        new TaskDependency("JOIN", "LEFT"),
                        new TaskDependency("JOIN", "RIGHT")
                )
        ));
        WorkflowDefinition published = workflows.publish(
                TenantId.LOCAL, created.id(), created.lockVersion()
        );
        var execution = executions.start(TenantId.LOCAL, published.id(), "kafka-result", TIME);
        UUID workflowExecutionId = execution.workflow().id();
        taskQueue.enqueueReadyTasks(TenantId.LOCAL, workflowExecutionId, 1000, TIME.plusSeconds(1));
        awaitListener(Duration.ofSeconds(15));

        try (KafkaProducer<String, String> producer = producer()) {
            ProducerRecord<String, String> root = resultRecord(workflowExecutionId, "ROOT");
            producer.send(root).get(10, TimeUnit.SECONDS);
            producer.send(root).get(10, TimeUnit.SECONDS);
            awaitStage(
                    workflowExecutionId,
                    1,
                    Map.of("ROOT", "SUCCEEDED", "LEFT", "RUNNING", "RIGHT", "RUNNING", "JOIN", "BLOCKED"),
                    true,
                    Duration.ofSeconds(15)
            );

            producer.send(resultRecord(workflowExecutionId, "LEFT")).get(10, TimeUnit.SECONDS);
            producer.send(resultRecord(workflowExecutionId, "RIGHT")).get(10, TimeUnit.SECONDS);
            awaitStage(
                    workflowExecutionId,
                    3,
                    Map.of("LEFT", "SUCCEEDED", "RIGHT", "SUCCEEDED", "JOIN", "RUNNING"),
                    false,
                    Duration.ofSeconds(15)
            );

            producer.send(resultRecord(workflowExecutionId, "JOIN")).get(10, TimeUnit.SECONDS);
        }

        awaitStage(
                workflowExecutionId,
                4,
                Map.of("JOIN", "SUCCEEDED"),
                false,
                Duration.ofSeconds(15)
        );
        var current = executions.findById(TenantId.LOCAL, workflowExecutionId).orElseThrow();
        assertThat(current.workflow().status()).isEqualTo(WorkflowRunStatus.SUCCEEDED);
        assertThat(current.tasks()).extracting(task -> task.status()).containsOnly(TaskRunStatus.SUCCEEDED);
        assertThat(jdbc.sql("""
                SELECT COUNT(*) FROM control_plane_result_inbox WHERE workflow_execution_id = :workflowId
                """).param("workflowId", workflowExecutionId).query(Long.class).single()).isEqualTo(4);
        assertThat(jdbc.sql("""
                SELECT COUNT(*) FROM control_plane_outbox
                 WHERE workflow_execution_id = :workflowId AND message_kind = 'TASK_COMMAND'
                """).param("workflowId", workflowExecutionId).query(Long.class).single()).isEqualTo(4);
    }

    @Test
    void routesPoisonResultsAndHeartbeatsToSeparateVersionedDeadLetterTopics() throws Exception {
        awaitListener(Duration.ofSeconds(15));
        UUID correlationId = UUID.randomUUID();
        try (KafkaConsumer<String, String> deadLetters = deadLetterConsumer();
             KafkaProducer<String, String> producer = producer()) {
            deadLetters.subscribe(List.of(
                    FlowForgeTopics.TASK_RESULTS_DLQ_V1,
                    FlowForgeTopics.TASK_HEARTBEATS_DLQ_V1
            ));
            producer.send(poisonRecord(
                    FlowForgeTopics.TASK_RESULTS_V1,
                    correlationId,
                    "invalid-result"
            )).get(10, TimeUnit.SECONDS);
            producer.send(poisonRecord(
                    FlowForgeTopics.TASK_HEARTBEATS_V1,
                    correlationId,
                    "invalid-heartbeat"
            )).get(10, TimeUnit.SECONDS);

            Map<String, org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> recovered =
                    awaitDeadLetters(deadLetters, Duration.ofSeconds(15));
            assertThat(recovered).containsOnlyKeys(
                    FlowForgeTopics.TASK_RESULTS_DLQ_V1,
                    FlowForgeTopics.TASK_HEARTBEATS_DLQ_V1
            );
            assertDeadLetter(
                    recovered.get(FlowForgeTopics.TASK_RESULTS_DLQ_V1),
                    FlowForgeTopics.TASK_RESULTS_V1,
                    "invalid-result",
                    correlationId
            );
            assertDeadLetter(
                    recovered.get(FlowForgeTopics.TASK_HEARTBEATS_DLQ_V1),
                    FlowForgeTopics.TASK_HEARTBEATS_V1,
                    "invalid-heartbeat",
                    correlationId
            );
        }

        assertThat(meters.get("flowforge.kafka.dlq.published")
                .tag("source_topic", FlowForgeTopics.TASK_RESULTS_V1)
                .counter().count()).isGreaterThanOrEqualTo(1.0);
        assertThat(meters.get("flowforge.kafka.dlq.published")
                .tag("source_topic", FlowForgeTopics.TASK_HEARTBEATS_V1)
                .counter().count()).isGreaterThanOrEqualTo(1.0);
    }

    private void awaitListener(Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            var containers = listeners.getListenerContainers();
            boolean assigned = !containers.isEmpty() && containers.stream()
                    .allMatch(container -> container.getAssignedPartitions() != null
                            && !container.getAssignedPartitions().isEmpty());
            if (assigned) return;
            Thread.sleep(100);
        }
        throw new AssertionError("Task-result listener did not receive a partition assignment");
    }

    private void awaitStage(
            UUID workflowExecutionId,
            long expectedInboxCount,
            Map<String, String> expectedTaskStatuses,
            boolean expectDuplicate,
            Duration timeout
    ) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            long inbox = jdbc.sql("""
                    SELECT COUNT(*) FROM control_plane_result_inbox
                     WHERE workflow_execution_id = :workflowId
                    """).param("workflowId", workflowExecutionId).query(Long.class).single();
            var duplicateCounter = meters.find("flowforge.results.consumed")
                    .tag("outcome", "DUPLICATE")
                    .counter();
            boolean duplicateObserved = !expectDuplicate
                    || (duplicateCounter != null && duplicateCounter.count() == 1);
            Map<String, String> statuses = jdbc.sql("""
                    SELECT task_key, status FROM task_execution
                     WHERE workflow_execution_id = :workflowId
                    """).param("workflowId", workflowExecutionId)
                    .query((rs, rowNum) -> Map.entry(rs.getString("task_key"), rs.getString("status")))
                    .list().stream()
                    .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            boolean taskStatusesMatch = expectedTaskStatuses.entrySet().stream()
                    .allMatch(entry -> entry.getValue().equals(statuses.get(entry.getKey())));
            if (inbox == expectedInboxCount && duplicateObserved && taskStatusesMatch) return;
            Thread.sleep(100);
        }
        throw new AssertionError("Kafka task results did not reach the expected DAG stage before timeout");
    }

    private ProducerRecord<String, String> resultRecord(UUID workflowExecutionId, String taskKey) throws Exception {
        var task = executions.findById(TenantId.LOCAL, workflowExecutionId).orElseThrow().tasks().stream()
                .filter(candidate -> candidate.taskKey().equals(taskKey))
                .findFirst()
                .orElseThrow();
        MessageEnvelope<TaskResultV1> envelope = new MessageEnvelope<>(
                UUID.randomUUID(),
                TaskResultV1.EVENT_TYPE,
                TaskResultV1.SCHEMA_VERSION,
                Instant.now(),
                workflowExecutionId,
                new TaskResultV1(
                        workflowExecutionId,
                        task.id(),
                        task.taskKey(),
                        task.stateVersion(),
                        1,
                        TaskResultOutcomeV1.SUCCEEDED,
                        null,
                        null,
                        jdbc.sql("""
                                SELECT fencing_token FROM task_attempt
                                 WHERE task_execution_id = :taskId AND status = 'RUNNING'
                                """).param("taskId", task.id()).query(UUID.class).single(),
                        null
                )
        );
        return new ProducerRecord<>(
                FlowForgeTopics.TASK_RESULTS_V1,
                workflowExecutionId.toString(),
                objectMapper.writeValueAsString(envelope)
        );
    }

    private static KafkaProducer<String, String> producer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }

    private static KafkaConsumer<String, String> deadLetterConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "control-plane-dlq-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(properties);
    }

    private static ProducerRecord<String, String> poisonRecord(
            String topic,
            UUID correlationId,
            String payload
    ) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, correlationId.toString(), payload);
        record.headers().add(
                "flowforge-correlation-id",
                correlationId.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)
        );
        return record;
    }

    private static Map<String, org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> awaitDeadLetters(
            KafkaConsumer<String, String> consumer,
            Duration timeout
    ) {
        Map<String, org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> recovered =
                new java.util.HashMap<>();
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline) && recovered.size() < 2) {
            consumer.poll(Duration.ofMillis(250)).forEach(record -> recovered.put(record.topic(), record));
        }
        if (recovered.size() != 2) throw new AssertionError("Poison result and heartbeat were not both dead-lettered");
        return recovered;
    }

    private static void assertDeadLetter(
            org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record,
            String sourceTopic,
            String payload,
            UUID correlationId
    ) {
        assertThat(record.value()).isEqualTo(payload);
        assertThat(header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(sourceTopic);
        assertThat(header(record, "flowforge-correlation-id")).isEqualTo(correlationId.toString());
        assertThat(header(record, "flowforge-dlq-schema-version")).isEqualTo("1");
        assertThat(header(record, "flowforge-dlq-record-id")).startsWith(sourceTopic + ":");
        assertThat(header(record, "flowforge-dlq-failure-class")).endsWith("StreamReadException");
    }

    private static String header(
            org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record,
            String name
    ) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
    }

    private static TaskDefinition task(String key) {
        return new TaskDefinition(key, key, "NOOP", Map.of());
    }
}
