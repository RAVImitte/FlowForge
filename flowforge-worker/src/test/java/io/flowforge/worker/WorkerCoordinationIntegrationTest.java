package io.flowforge.worker;

import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskCommandV1;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
class WorkerCoordinationIntegrationTest {
    private static final int PARTITION_COUNT = 6;

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @Test
    void twoWorkersSharePartitionsAndProcessOnlyTheirAssignments() throws Exception {
        createTopics();
        try (ConfigurableApplicationContext workerA = startWorker("worker-a");
             ConfigurableApplicationContext workerB = startWorker("worker-b")) {
            AssignmentPair assignments = awaitBalancedAssignments(workerA, workerB, Duration.ofSeconds(20));
            Set<TopicPartition> assignmentsA = assignments.workerA();
            Set<TopicPartition> assignmentsB = assignments.workerB();

            assertThat(assignmentsA).isNotEmpty();
            assertThat(assignmentsB).isNotEmpty();
            assertThat(assignmentsA).doesNotContainAnyElementsOf(assignmentsB);
            Set<TopicPartition> allAssignments = new HashSet<>(assignmentsA);
            allAssignments.addAll(assignmentsB);
            assertThat(allAssignments).hasSize(PARTITION_COUNT);

            ObjectMapper objectMapper = workerA.getBean(ObjectMapper.class);
            sendOneCommandPerPartition(objectMapper);

            JdbcClient jdbc = workerA.getBean(JdbcClient.class);
            awaitCompleted(jdbc, PARTITION_COUNT, Duration.ofSeconds(20));
            List<String> workers = jdbc.sql("""
                    SELECT DISTINCT completed_by
                      FROM worker_command_inbox
                     WHERE status = 'COMPLETED'
                     ORDER BY completed_by
                    """).query(String.class).list();
            assertThat(workers).containsExactly("worker-a", "worker-b");
        }
    }

    private static ConfigurableApplicationContext startWorker(String workerId) {
        return new SpringApplicationBuilder(FlowForgeWorkerApplication.class)
                .web(WebApplicationType.NONE)
                .run(
                        "--spring.main.banner-mode=off",
                        "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                        "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                        "--spring.datasource.username=" + POSTGRES.getUsername(),
                        "--spring.datasource.password=" + POSTGRES.getPassword(),
                        "--flowforge.worker.id=" + workerId,
                        "--flowforge.worker.consumer-group=worker-coordination-test",
                        "--flowforge.worker.execution.concurrency=1",
                        "--flowforge.worker.execution.result-poll-interval-ms=50",
                        "--logging.level.root=WARN"
                );
    }

    private static AssignmentPair awaitBalancedAssignments(
            ConfigurableApplicationContext workerA,
            ConfigurableApplicationContext workerB,
            Duration timeout
    ) throws InterruptedException {
        KafkaListenerEndpointRegistry registryA = workerA.getBean(KafkaListenerEndpointRegistry.class);
        KafkaListenerEndpointRegistry registryB = workerB.getBean(KafkaListenerEndpointRegistry.class);
        Instant deadline = Instant.now().plus(timeout);
        Set<TopicPartition> assignmentsA = Set.of();
        Set<TopicPartition> assignmentsB = Set.of();
        while (Instant.now().isBefore(deadline)) {
            assignmentsA = assignments(registryA);
            assignmentsB = assignments(registryB);
            Set<TopicPartition> union = new HashSet<>(assignmentsA);
            union.addAll(assignmentsB);
            Set<TopicPartition> overlap = new HashSet<>(assignmentsA);
            overlap.retainAll(assignmentsB);
            if (!assignmentsA.isEmpty()
                    && !assignmentsB.isEmpty()
                    && overlap.isEmpty()
                    && union.size() == PARTITION_COUNT) {
                return new AssignmentPair(assignmentsA, assignmentsB);
            }
            Thread.sleep(100);
        }
        throw new AssertionError(
                "Workers did not reach a balanced partition assignment: " + assignmentsA + " / " + assignmentsB
        );
    }

    private static Set<TopicPartition> assignments(KafkaListenerEndpointRegistry registry) {
        return registry.getListenerContainers().stream()
                .flatMap(container -> container.getAssignedPartitions().stream())
                .collect(java.util.stream.Collectors.toSet());
    }

    private static void createTopics() throws Exception {
        Map<String, Object> properties = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()
        );
        try (AdminClient admin = AdminClient.create(properties)) {
            admin.createTopics(List.of(
                    new NewTopic(FlowForgeTopics.TASK_COMMANDS_V1, PARTITION_COUNT, (short) 1),
                    new NewTopic(FlowForgeTopics.TASK_RESULTS_V1, PARTITION_COUNT, (short) 1)
            )).all().get(15, TimeUnit.SECONDS);
        }
    }

    private static void sendOneCommandPerPartition(ObjectMapper objectMapper) throws Exception {
        try (KafkaProducer<String, String> producer = producer()) {
            for (int partition = 0; partition < PARTITION_COUNT; partition++) {
                UUID workflowId = UUID.randomUUID();
                UUID taskId = UUID.randomUUID();
                MessageEnvelope<TaskCommandV1> command = new MessageEnvelope<>(
                        UUID.randomUUID(),
                        TaskCommandV1.EVENT_TYPE,
                        TaskCommandV1.SCHEMA_VERSION,
                        Instant.now(),
                        workflowId,
                        new TaskCommandV1(
                                workflowId,
                                taskId,
                                "TASK_" + partition,
                                "NOOP",
                                Map.of(),
                                1,
                                1
                        )
                );
                producer.send(new ProducerRecord<>(
                        FlowForgeTopics.TASK_COMMANDS_V1,
                        partition,
                        taskId.toString(),
                        objectMapper.writeValueAsString(command)
                )).get(10, TimeUnit.SECONDS);
            }
        }
    }

    private static KafkaProducer<String, String> producer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }

    private static void awaitCompleted(JdbcClient jdbc, long expected, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            long completed = jdbc.sql("""
                    SELECT COUNT(*) FROM worker_command_inbox WHERE status = 'COMPLETED'
                    """).query(Long.class).single();
            if (completed == expected) return;
            Thread.sleep(100);
        }
        throw new AssertionError("Workers did not complete all partitioned commands before timeout");
    }

    private record AssignmentPair(Set<TopicPartition> workerA, Set<TopicPartition> workerB) {
    }
}
