package io.flowforge.worker;

import io.flowforge.application.execution.DurableTaskQueue;
import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.controlplane.FlowForgeApplication;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.TaskCommandV1;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
class DistributedRestartRecoveryIntegrationTest {
    private static final String RESULT_GROUP = "restart-recovery-results";
    private static final String WORKER_GROUP = "restart-recovery-workers";

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @Test
    void recoversControlPlaneOutboxWorkerResultAndOfflineConsumerAcrossRestarts() throws Exception {
        createTopics();
        UUID executionId = seedPendingTaskCommand();

        try (ConfigurableApplicationContext controlPlane = startDistributedControlPlane("control-a")) {
            JdbcClient jdbc = controlPlane.getBean(JdbcClient.class);
            awaitCount(jdbc, """
                    SELECT COUNT(*) FROM control_plane_outbox
                     WHERE workflow_execution_id = :executionId
                       AND event_type = :eventType
                       AND status = 'PUBLISHED'
                    """, executionId, TaskCommandV1.EVENT_TYPE, 1, Duration.ofSeconds(20));

            try (ConfigurableApplicationContext worker = startWorker("worker-a", false)) {
                awaitCount(worker.getBean(JdbcClient.class), """
                        SELECT COUNT(*) FROM worker_command_inbox
                         WHERE workflow_execution_id = :executionId
                           AND status = 'COMPLETED'
                        """, executionId, null, 1, Duration.ofSeconds(20));
                awaitCount(worker.getBean(JdbcClient.class), """
                        SELECT COUNT(*) FROM worker_result_outbox
                         WHERE workflow_execution_id = :executionId
                           AND status = 'PENDING'
                        """, executionId, null, 1, Duration.ofSeconds(10));
            }
        }

        try (ConfigurableApplicationContext worker = startWorker("worker-b", true)) {
            awaitCount(worker.getBean(JdbcClient.class), """
                    SELECT COUNT(*) FROM worker_result_outbox
                     WHERE workflow_execution_id = :executionId
                       AND status = 'PUBLISHED'
                    """, executionId, null, 1, Duration.ofSeconds(20));
        }

        try (ConfigurableApplicationContext controlPlane = startDistributedControlPlane("control-b")) {
            JdbcClient jdbc = controlPlane.getBean(JdbcClient.class);
            awaitCount(jdbc, """
                    SELECT COUNT(*) FROM workflow_execution
                     WHERE id = :executionId AND status = 'SUCCEEDED'
                    """, executionId, null, 1, Duration.ofSeconds(20));

            assertThat(count(jdbc, """
                    SELECT COUNT(*) FROM control_plane_result_inbox
                     WHERE workflow_execution_id = :executionId AND disposition = 'APPLIED'
                    """, executionId, null)).isEqualTo(1);
            assertThat(count(jdbc, """
                    SELECT COUNT(*) FROM worker_command_inbox
                     WHERE workflow_execution_id = :executionId AND completed_by = 'worker-a'
                    """, executionId, null)).isEqualTo(1);
            assertThat(count(jdbc, """
                    SELECT COUNT(*) FROM worker_command_inbox
                     WHERE workflow_execution_id = :executionId
                    """, executionId, null)).isEqualTo(1);
        }
    }

    private static UUID seedPendingTaskCommand() {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(FlowForgeApplication.class)
                .web(WebApplicationType.NONE)
                .run(controlPlaneArguments(
                        "--flowforge.kafka.enabled=false",
                        "--flowforge.execution.dispatch-enabled=false",
                        "--flowforge.outbox.publisher-enabled=false",
                        "--flowforge.outbox.command-dispatch-enabled=false",
                        "--flowforge.results.consumer-enabled=false"
                ))) {
            WorkflowService workflows = context.getBean(WorkflowService.class);
            WorkflowDefinition draft = workflows.create(new WorkflowDraft(
                    "Restart recovery",
                    "Verifies recovery across independently restarted processes",
                    List.of(new TaskDefinition("ROOT", "Root", "NOOP", Map.of())),
                    List.of()
            ));
            WorkflowDefinition published = workflows.publish(draft.id(), draft.lockVersion());
            var execution = context.getBean(ExecutionRepository.class)
                    .start(published.id(), "restart-recovery", Instant.now());
            UUID executionId = execution.workflow().id();

            assertThat(context.getBean(DurableTaskQueue.class)
                    .enqueueReadyTasks(executionId, 10, Instant.now())).isEqualTo(1);
            assertThat(count(context.getBean(JdbcClient.class), """
                    SELECT COUNT(*) FROM control_plane_outbox
                     WHERE workflow_execution_id = :executionId
                       AND event_type = :eventType
                       AND status = 'PENDING'
                    """, executionId, TaskCommandV1.EVENT_TYPE)).isEqualTo(1);
            return executionId;
        }
    }

    private static ConfigurableApplicationContext startDistributedControlPlane(String instanceId) {
        return new SpringApplicationBuilder(FlowForgeApplication.class)
                .web(WebApplicationType.NONE)
                .run(controlPlaneArguments(
                        "--flowforge.kafka.enabled=true",
                        "--flowforge.execution.dispatch-enabled=false",
                        "--flowforge.execution.require-active-dispatcher=true",
                        "--flowforge.outbox.publisher-enabled=true",
                        "--flowforge.outbox.command-dispatch-enabled=true",
                        "--flowforge.results.consumer-enabled=true",
                        "--flowforge.leases.heartbeat-consumer-enabled=true",
                        "--flowforge.outbox.instance-id=" + instanceId,
                        "--flowforge.results.consumer-group=" + RESULT_GROUP,
                        "--flowforge.outbox.poll-interval-ms=50",
                        "--flowforge.outbox.command-dispatch-interval-ms=50"
                ));
    }

    private static ConfigurableApplicationContext startWorker(String workerId, boolean resultPublisherEnabled) {
        return new SpringApplicationBuilder(FlowForgeWorkerApplication.class)
                .web(WebApplicationType.NONE)
                .run(
                        "--spring.main.banner-mode=off",
                        "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                        "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                        "--spring.datasource.username=" + POSTGRES.getUsername(),
                        "--spring.datasource.password=" + POSTGRES.getPassword(),
                        "--flowforge.worker.id=" + workerId,
                        "--flowforge.worker.consumer-group=" + WORKER_GROUP,
                        "--flowforge.worker.result-publisher-enabled=" + resultPublisherEnabled,
                        "--flowforge.worker.execution.result-poll-interval-ms=50",
                        "--logging.level.root=WARN"
                );
    }

    private static String[] controlPlaneArguments(String... additional) {
        String[] common = {
                "--spring.main.banner-mode=off",
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--spring.flyway.locations=classpath:db/migration",
                "--spring.flyway.table=flyway_schema_history",
                "--spring.flyway.baseline-on-migrate=true",
                "--spring.flyway.baseline-version=0",
                "--flowforge.kafka.topics.partitions=6",
                "--flowforge.kafka.topics.replication-factor=1",
                "--flowforge.outbox.batch-size=100",
                "--flowforge.outbox.lease-duration=30s",
                "--flowforge.outbox.publish-timeout=10s",
                "--flowforge.outbox.instance-id=restart-seed",
                "--flowforge.results.enqueue-batch-size=1000",
                "--logging.level.root=WARN"
        };
        String[] combined = new String[common.length + additional.length];
        System.arraycopy(common, 0, combined, 0, common.length);
        System.arraycopy(additional, 0, combined, common.length, additional.length);
        return combined;
    }

    private static void createTopics() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()
        ))) {
            admin.createTopics(List.of(
                    new NewTopic(FlowForgeTopics.TASK_COMMANDS_V1, 6, (short) 1),
                    new NewTopic(FlowForgeTopics.TASK_RESULTS_V1, 6, (short) 1),
                    new NewTopic(FlowForgeTopics.EXECUTION_EVENTS_V1, 6, (short) 1)
            )).all().get(15, TimeUnit.SECONDS);
        }
    }

    private static void awaitCount(
            JdbcClient jdbc,
            String sql,
            UUID executionId,
            String eventType,
            long expected,
            Duration timeout
    ) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (count(jdbc, sql, executionId, eventType) == expected) return;
            Thread.sleep(100);
        }
        throw new AssertionError("Expected count " + expected + " before timeout");
    }

    private static long count(JdbcClient jdbc, String sql, UUID executionId, String eventType) {
        JdbcClient.StatementSpec statement = jdbc.sql(sql).param("executionId", executionId);
        if (eventType != null) statement = statement.param("eventType", eventType);
        return statement.query(Long.class).single();
    }
}
