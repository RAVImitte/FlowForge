package io.flowforge.worker;

import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.FlowForgeHeaders;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskCommandV1;
import io.flowforge.messaging.TaskResultOutcomeV1;
import io.flowforge.messaging.TaskResultV1;
import io.flowforge.worker.application.WorkerTaskHandler;
import io.flowforge.worker.application.WorkerTaskResult;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "flowforge.worker.id=worker-kafka-test",
                "flowforge.worker.execution.result-poll-interval-ms=50",
                "flowforge.kafka.recovery.max-retries=1",
                "flowforge.kafka.recovery.initial-backoff=10ms",
                "flowforge.kafka.recovery.max-backoff=10ms"
        }
)
@Testcontainers(disabledWithoutDocker = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Import(WorkerKafkaIntegrationTest.TestHandlerConfiguration.class)
class WorkerKafkaIntegrationTest {
    private static final String TRACE_ID = "fedcba9876543210fedcba9876543210";
    private static final String TRACE_PARENT = "00-" + TRACE_ID + "-0123456789abcdef-01";
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    ApplicationContext context;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    KafkaListenerEndpointRegistry listenerRegistry;

    @Autowired
    AtomicInteger handlerExecutions;

    @Autowired
    MeterRegistry meters;

    @Test
    void reportsKafkaAndWorkerReadiness() {
        HealthIndicator kafka = context.getBean("kafka", HealthIndicator.class);
        HealthIndicator worker = context.getBean("worker", HealthIndicator.class);

        assertThat(kafka.health().getStatus().getCode()).isEqualTo("UP");
        assertThat(kafka.health().getDetails()).containsKey("clusterId").containsEntry("nodes", 1);
        assertThat(worker.health().getStatus().getCode()).isEqualTo("UP");
    }

    @Test
    void storesCommandBeforeExecutionAndReusesResultForDuplicateDelivery() throws Exception {
        waitForListenerAssignment();
        int executionsBefore = handlerExecutions.get();
        UUID workflowId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        MessageEnvelope<TaskCommandV1> command = new MessageEnvelope<>(
                UUID.randomUUID(),
                TaskCommandV1.EVENT_TYPE,
                TaskCommandV1.SCHEMA_VERSION,
                Instant.now(),
                workflowId,
                "merchant-a",
                new TaskCommandV1(workflowId, taskId, "COUNT", "COUNTING", Map.of(), 1, 1)
        );
        String payload = objectMapper.writeValueAsString(command);

        try (KafkaConsumer<String, String> results = resultConsumer();
             KafkaProducer<String, String> commands = commandProducer()) {
            results.subscribe(List.of(FlowForgeTopics.TASK_RESULTS_V1));
            commands.send(tracedCommand(taskId, payload)).get(10, TimeUnit.SECONDS);
            commands.send(tracedCommand(taskId, payload)).get(10, TimeUnit.SECONDS);

            awaitCompleted(command.eventId(), Duration.ofSeconds(15));
            var records = results.poll(Duration.ofSeconds(10));
            var matching = java.util.stream.StreamSupport.stream(records.spliterator(), false)
                    .filter(record -> workflowId.toString().equals(header(record, "flowforge-correlation-id")))
                    .toList();
            assertThat(matching).hasSize(1);
            MessageEnvelope<TaskResultV1> result = objectMapper.readValue(
                    matching.getFirst().value(),
                    new TypeReference<MessageEnvelope<TaskResultV1>>() {
                    }
            );
            assertThat(result.payload().outcome()).isEqualTo(TaskResultOutcomeV1.SUCCEEDED);
            assertThat(result.tenantId()).isEqualTo("merchant-a");
            assertThat(header(matching.getFirst(), FlowForgeHeaders.TENANT_ID)).isEqualTo("merchant-a");
            assertThat(header(matching.getFirst(), "traceparent")).contains("-" + TRACE_ID + "-");
            assertThat(header(matching.getFirst(), "baggage")).isEqualTo("tenant=portfolio");
            assertThat(results.poll(Duration.ofSeconds(1)).isEmpty()).isTrue();
        }

        assertThat(handlerExecutions.get()).isEqualTo(executionsBefore + 1);
        assertThat(jdbc.sql("SELECT COUNT(*) FROM worker_command_inbox WHERE event_id = :eventId")
                .param("eventId", command.eventId()).query(Long.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT COUNT(*) FROM worker_result_outbox WHERE command_event_id = :eventId")
                .param("eventId", command.eventId()).query(Long.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT trace_parent FROM worker_result_outbox WHERE command_event_id = :eventId")
                .param("eventId", command.eventId()).query(String.class).single())
                .contains("-" + TRACE_ID + "-");
    }

    @Test
    void deadLettersPoisonCommandThenContinuesThePartition() throws Exception {
        waitForListenerAssignment();
        int executionsBefore = handlerExecutions.get();
        UUID workflowId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        MessageEnvelope<TaskCommandV1> valid = new MessageEnvelope<>(
                UUID.randomUUID(),
                TaskCommandV1.EVENT_TYPE,
                TaskCommandV1.SCHEMA_VERSION,
                Instant.now(),
                workflowId,
                new TaskCommandV1(workflowId, taskId, "AFTER_POISON", "COUNTING", Map.of(), 1, 1)
        );

        try (KafkaConsumer<String, String> deadLetters = deadLetterConsumer();
             KafkaProducer<String, String> commands = commandProducer()) {
            deadLetters.subscribe(List.of(FlowForgeTopics.TASK_COMMANDS_DLQ_V1));
            ProducerRecord<String, String> poison = new ProducerRecord<>(
                    FlowForgeTopics.TASK_COMMANDS_V1,
                    taskId.toString(),
                    "{not-json"
            );
            poison.headers().add(
                    "flowforge-correlation-id",
                    workflowId.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)
            );
            poison.headers().add(
                    FlowForgeHeaders.TENANT_ID,
                    "merchant-poison".getBytes(java.nio.charset.StandardCharsets.UTF_8)
            );
            commands.send(poison).get(10, TimeUnit.SECONDS);
            commands.send(new ProducerRecord<>(
                    FlowForgeTopics.TASK_COMMANDS_V1,
                    taskId.toString(),
                    objectMapper.writeValueAsString(valid)
            )).get(10, TimeUnit.SECONDS);

            var deadLetter = awaitDeadLetter(deadLetters, Duration.ofSeconds(15));
            assertThat(deadLetter.topic()).isEqualTo(FlowForgeTopics.TASK_COMMANDS_DLQ_V1);
            assertThat(deadLetter.key()).isEqualTo(taskId.toString());
            assertThat(deadLetter.value()).isEqualTo("{not-json");
            assertThat(header(deadLetter, KafkaHeaders.DLT_ORIGINAL_TOPIC))
                    .isEqualTo(FlowForgeTopics.TASK_COMMANDS_V1);
            assertThat(header(deadLetter, "flowforge-correlation-id")).isEqualTo(workflowId.toString());
            assertThat(header(deadLetter, FlowForgeHeaders.TENANT_ID)).isEqualTo("merchant-poison");
            assertThat(header(deadLetter, "flowforge-dlq-schema-version")).isEqualTo("1");
            assertThat(header(deadLetter, "flowforge-dlq-record-id"))
                    .startsWith(FlowForgeTopics.TASK_COMMANDS_V1 + ":");
            assertThat(header(deadLetter, "flowforge-dlq-failure-class"))
                    .endsWith("StreamReadException");
        }

        awaitCompleted(valid.eventId(), Duration.ofSeconds(15));
        assertThat(handlerExecutions.get()).isEqualTo(executionsBefore + 1);
        assertThat(meters.get("flowforge.kafka.dlq.published")
                .tag("source_topic", FlowForgeTopics.TASK_COMMANDS_V1)
                .counter().count()).isGreaterThanOrEqualTo(1.0);
    }

    private void waitForListenerAssignment() throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(15);
        while (Instant.now().isBefore(deadline)) {
            boolean assigned = listenerRegistry.getListenerContainers().stream()
                    .anyMatch(container -> container.getAssignedPartitions() != null
                            && !container.getAssignedPartitions().isEmpty());
            if (assigned) return;
            Thread.sleep(100);
        }
        throw new AssertionError("Worker Kafka listener did not receive a partition assignment");
    }

    private void awaitCompleted(UUID eventId, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            Long completed = jdbc.sql("""
                    SELECT COUNT(*) FROM worker_command_inbox
                     WHERE event_id = :eventId AND status = 'COMPLETED'
                    """).param("eventId", eventId).query(Long.class).single();
            Long published = jdbc.sql("""
                    SELECT COUNT(*) FROM worker_result_outbox
                     WHERE command_event_id = :eventId AND status = 'PUBLISHED'
                    """).param("eventId", eventId).query(Long.class).single();
            if (completed == 1 && published == 1) return;
            Thread.sleep(100);
        }
        throw new AssertionError("Worker command did not complete and publish before timeout");
    }

    private static KafkaProducer<String, String> commandProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }

    private static ProducerRecord<String, String> tracedCommand(UUID taskId, String payload) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                FlowForgeTopics.TASK_COMMANDS_V1, taskId.toString(), payload
        );
        record.headers().add("traceparent", TRACE_PARENT.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        record.headers().add("baggage", "tenant=portfolio".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        record.headers().add(FlowForgeHeaders.TENANT_ID, "merchant-a".getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private static KafkaConsumer<String, String> resultConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "worker-result-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(properties);
    }

    private static KafkaConsumer<String, String> deadLetterConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "worker-dlq-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(properties);
    }

    private static org.apache.kafka.clients.consumer.ConsumerRecord<String, String> awaitDeadLetter(
            KafkaConsumer<String, String> consumer,
            Duration timeout
    ) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            var records = consumer.poll(Duration.ofMillis(250));
            if (!records.isEmpty()) return records.iterator().next();
        }
        throw new AssertionError("Poison command was not published to the command DLQ");
    }

    private static String header(
            org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record,
            String name
    ) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TestHandlerConfiguration {
        @Bean
        NewTopic taskCommandsTopic() {
            return new NewTopic(FlowForgeTopics.TASK_COMMANDS_V1, 1, (short) 1);
        }

        @Bean
        NewTopic taskResultsTopic() {
            return new NewTopic(FlowForgeTopics.TASK_RESULTS_V1, 1, (short) 1);
        }

        @Bean
        NewTopic taskCommandsDeadLetterTopic() {
            return new NewTopic(FlowForgeTopics.TASK_COMMANDS_DLQ_V1, 1, (short) 1);
        }

        @Bean
        AtomicInteger handlerExecutions() {
            return new AtomicInteger();
        }

        @Bean
        WorkerTaskHandler countingTaskHandler(AtomicInteger executions) {
            return new WorkerTaskHandler() {
                @Override
                public String taskType() {
                    return "COUNTING";
                }

                @Override
                public WorkerTaskResult execute(io.flowforge.worker.application.TaskExecutionContext context) {
                    executions.incrementAndGet();
                    return WorkerTaskResult.succeeded();
                }
            };
        }
    }
}
