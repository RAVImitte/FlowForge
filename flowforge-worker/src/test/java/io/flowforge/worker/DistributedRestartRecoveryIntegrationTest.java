package io.flowforge.worker;

import io.flowforge.application.coordination.EphemeralPermitStore;
import io.flowforge.application.schedule.ScheduleFireRepository;
import io.flowforge.application.schedule.WorkflowScheduleService;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.controlplane.FlowForgeApplication;
import io.flowforge.domain.schedule.MisfirePolicy;
import io.flowforge.domain.schedule.OneTimeSchedule;
import io.flowforge.domain.schedule.WorkflowScheduleDraft;
import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.TaskReliabilityPolicy;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.sql.Timestamp;
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
    private static final Duration DISTRIBUTED_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration MIRROR_TIMEOUT = Duration.ofSeconds(30);

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.4-alpine")
            .withExposedPorts(6379);

    @Test
    void recoversSchedulePermitsOutboxesAndOfflineConsumersAcrossRestarts() throws Exception {
        createTopics();
        SeededSchedule seed = seedAbandonedScheduleFire();
        UUID executionId;
        List<String> permitResources;

        try (ConfigurableApplicationContext controlPlane = startDistributedControlPlane("control-a", true)) {
            JdbcClient jdbc = controlPlane.getBean(JdbcClient.class);
            executionId = awaitExecution(jdbc, seed.workflowId(), DISTRIBUTED_TIMEOUT);
            awaitCount(jdbc, """
                    SELECT COUNT(*) FROM workflow_schedule_trigger
                     WHERE id = :executionId AND status = 'STARTED'
                    """, seed.triggerId(), null, 1, DISTRIBUTED_TIMEOUT);
            try {
                awaitCount(jdbc, """
                        SELECT COUNT(*) FROM control_plane_outbox
                         WHERE workflow_execution_id = :executionId
                           AND event_type = :eventType
                           AND status = 'PUBLISHED'
                        """, executionId, TaskCommandV1.EVENT_TYPE, 1, DISTRIBUTED_TIMEOUT);
            } catch (AssertionError failure) {
                throw new AssertionError(failure.getMessage() + "; " + executionDiagnostics(jdbc, executionId), failure);
            }
            permitResources = activePermitResources(jdbc, executionId);
            assertThat(permitResources).hasSize(2);
            awaitPermitMirrors(controlPlane, permitResources, MIRROR_TIMEOUT);

            try (ConfigurableApplicationContext worker = startWorker("worker-a", false)) {
                awaitCount(worker.getBean(JdbcClient.class), """
                        SELECT COUNT(*) FROM worker_command_inbox
                         WHERE workflow_execution_id = :executionId
                           AND status = 'COMPLETED'
                        """, executionId, null, 1, DISTRIBUTED_TIMEOUT);
                awaitCount(worker.getBean(JdbcClient.class), """
                        SELECT COUNT(*) FROM worker_result_outbox
                         WHERE workflow_execution_id = :executionId
                           AND status = 'PENDING'
                        """, executionId, null, 1, MIRROR_TIMEOUT);
            }
        }

        assertThat(REDIS.execInContainer("redis-cli", "FLUSHALL").getExitCode()).isZero();
        try (ConfigurableApplicationContext replacement = startDistributedControlPlane("control-reconcile", false)) {
            awaitPermitMirrors(replacement, permitResources, MIRROR_TIMEOUT);
            assertThat(activePermitResources(replacement.getBean(JdbcClient.class), executionId)).hasSize(2);
        }

        try (ConfigurableApplicationContext worker = startWorker("worker-b", true)) {
            awaitCount(worker.getBean(JdbcClient.class), """
                    SELECT COUNT(*) FROM worker_result_outbox
                     WHERE workflow_execution_id = :executionId
                       AND status = 'PUBLISHED'
                    """, executionId, null, 1, DISTRIBUTED_TIMEOUT);
        }

        try (ConfigurableApplicationContext controlPlane = startDistributedControlPlane("control-b", true)) {
            JdbcClient jdbc = controlPlane.getBean(JdbcClient.class);
            awaitCount(jdbc, """
                    SELECT COUNT(*) FROM workflow_execution
                     WHERE id = :executionId AND status = 'SUCCEEDED'
                    """, executionId, null, 1, DISTRIBUTED_TIMEOUT);

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
            assertThat(activePermitResources(jdbc, executionId)).isEmpty();
            assertThat(count(jdbc, """
                    SELECT COUNT(*) FROM workflow_execution
                     WHERE workflow_id = :executionId
                    """, seed.workflowId(), null)).isEqualTo(1);
        }
    }

    private static SeededSchedule seedAbandonedScheduleFire() {
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
            WorkflowDefinition draft = workflows.create(TenantId.LOCAL, new WorkflowDraft(
                    "Restart recovery",
                    "Verifies recovery across independently restarted processes",
                    List.of(new TaskDefinition(
                            "ROOT",
                            "Root",
                            "NOOP",
                            Map.of(),
                            TaskReliabilityPolicy.defaults(),
                            1
                    )),
                    List.of(),
                    1
            ));
            WorkflowDefinition published = workflows.publish(
                    TenantId.LOCAL, draft.id(), draft.lockVersion());
            var schedule = context.getBean(WorkflowScheduleService.class).create(
                    TenantId.LOCAL, new WorkflowScheduleDraft(
                    published.id(),
                    new OneTimeSchedule(Instant.now().plus(Duration.ofHours(1))),
                    MisfirePolicy.FIRE_ONCE
            ));
            Instant now = Instant.now();
            JdbcClient jdbc = context.getBean(JdbcClient.class);
            Instant due = now.minusSeconds(1);
            jdbc.sql("""
                    UPDATE workflow_schedule SET one_time_at = :due, next_fire_at = :due WHERE id = :id
                    """)
                    .param("due", Timestamp.from(due))
                    .param("id", schedule.id())
                    .update();

            ScheduleFireRepository fires = context.getBean(ScheduleFireRepository.class);
            assertThat(fires.materializeDue(1, now, Duration.ofMinutes(1)).pending()).isEqualTo(1);
            var abandoned = fires.claimPending(
                    TenantId.LOCAL, 1, "failed-scheduler", now, Duration.ofMillis(1)
            ).getFirst();
            return new SeededSchedule(published.id(), abandoned.triggerId());
        }
    }

    private static ConfigurableApplicationContext startDistributedControlPlane(
            String instanceId,
            boolean resultConsumerEnabled
    ) {
        return new SpringApplicationBuilder(FlowForgeApplication.class)
                .web(WebApplicationType.NONE)
                .run(controlPlaneArguments(
                        "--flowforge.kafka.enabled=true",
                        "--flowforge.execution.dispatch-enabled=false",
                        "--flowforge.execution.require-active-dispatcher=" + resultConsumerEnabled,
                        "--flowforge.outbox.publisher-enabled=true",
                        "--flowforge.outbox.command-dispatch-enabled=" + resultConsumerEnabled,
                        "--flowforge.results.consumer-enabled=" + resultConsumerEnabled,
                        "--flowforge.leases.heartbeat-consumer-enabled=true",
                        "--flowforge.scheduling.enabled=true",
                        "--flowforge.scheduling.poll-interval-ms=50",
                        "--flowforge.scheduling.lease-duration=5s",
                        "--flowforge.scheduling.instance-id=" + instanceId,
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
                         "--debug=false",
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
                "--spring.data.redis.host=" + REDIS.getHost(),
                "--spring.data.redis.port=" + REDIS.getMappedPort(6379),
                "--spring.flyway.locations=classpath:db/migration",
                "--spring.flyway.table=flyway_schema_history",
                "--spring.flyway.baseline-on-migrate=true",
                "--spring.flyway.baseline-version=0",
                "--flowforge.kafka.topics.partitions=6",
                "--flowforge.kafka.topics.replication-factor=1",
                "--flowforge.outbox.batch-size=100",
                "--flowforge.outbox.publish-concurrency=8",
                "--flowforge.outbox.lease-duration=30s",
                "--flowforge.outbox.publish-timeout=10s",
                "--flowforge.outbox.instance-id=restart-seed",
                "--flowforge.results.enqueue-batch-size=1000",
                "--flowforge.coordination.enabled=true",
                "--flowforge.coordination.namespace=restart-recovery",
                "--flowforge.coordination.lease-duration=2m",
                 "--flowforge.concurrency.lease-duration=2m",
                 "--flowforge.concurrency.reconciliation-enabled=true",
                 "--flowforge.concurrency.reconciliation-interval-ms=50",
                 "--debug=false",
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
        long actual = count(jdbc, sql, executionId, eventType);
        throw new AssertionError("Expected count " + expected + " before timeout, but was " + actual);
    }

    private static long count(JdbcClient jdbc, String sql, UUID executionId, String eventType) {
        JdbcClient.StatementSpec statement = jdbc.sql(sql).param("executionId", executionId);
        if (eventType != null) statement = statement.param("eventType", eventType);
        return statement.query(Long.class).single();
    }

    private static UUID awaitExecution(JdbcClient jdbc, UUID workflowId, Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            List<UUID> executions = jdbc.sql("""
                    SELECT id FROM workflow_execution WHERE workflow_id = :workflowId ORDER BY started_at
                    """)
                    .param("workflowId", workflowId)
                    .query(UUID.class)
                    .list();
            if (executions.size() == 1) return executions.getFirst();
            Thread.sleep(100);
        }
        throw new AssertionError("Replacement scheduler did not create exactly one workflow execution");
    }

    private static List<String> activePermitResources(JdbcClient jdbc, UUID executionId) {
        return jdbc.sql("""
                SELECT cp.resource_key
                  FROM coordination_permit cp
                 WHERE cp.status = 'ACTIVE'
                   AND (
                       cp.token = (
                           SELECT concurrency_permit_token FROM workflow_execution WHERE id = :executionId
                       )
                       OR cp.token IN (
                           SELECT ta.concurrency_permit_token
                             FROM task_attempt ta
                             JOIN task_execution te ON te.id = ta.task_execution_id
                            WHERE te.workflow_execution_id = :executionId
                              AND ta.concurrency_permit_token IS NOT NULL
                       )
                   )
                 ORDER BY cp.resource_key
                """)
                .param("executionId", executionId)
                .query(String.class)
                .list();
    }

    private static String executionDiagnostics(JdbcClient jdbc, UUID executionId) {
        List<String> tasks = jdbc.sql("""
                        SELECT task_key, status FROM task_execution
                         WHERE workflow_execution_id = :executionId ORDER BY task_key
                        """)
                .param("executionId", executionId)
                .query((resultSet, rowNumber) -> resultSet.getString("task_key")
                        + "=" + resultSet.getString("status"))
                .list();
        List<String> outbox = jdbc.sql("""
                        SELECT event_type, status, attempt_count, COALESCE(last_error, '') AS last_error
                          FROM control_plane_outbox
                         WHERE workflow_execution_id = :executionId ORDER BY created_at, id
                        """)
                .param("executionId", executionId)
                .query((resultSet, rowNumber) -> resultSet.getString("event_type")
                        + "=" + resultSet.getString("status")
                        + "#" + resultSet.getInt("attempt_count")
                        + (resultSet.getString("last_error").isEmpty()
                        ? ""
                        : "(" + resultSet.getString("last_error") + ")"))
                .list();
        return "tasks=" + tasks + ", outbox=" + outbox;
    }

    private static void awaitPermitMirrors(
            ConfigurableApplicationContext context,
            List<String> resources,
            Duration timeout
    ) throws InterruptedException {
        EphemeralPermitStore store = context.getBean(EphemeralPermitStore.class);
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (resources.stream().allMatch(resource ->
                    store.activeCount(TenantId.LOCAL, resource, Instant.now()) == 1)) return;
            Thread.sleep(100);
        }
        throw new AssertionError("Redis permit mirrors were not reconstructed from PostgreSQL");
    }

    private record SeededSchedule(UUID workflowId, UUID triggerId) {
    }
}
