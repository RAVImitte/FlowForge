package io.flowforge.controlplane;

import io.flowforge.application.execution.AttemptLeaseRecovery;
import io.flowforge.application.execution.ExecutionConflictException;
import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.execution.InboundTaskHeartbeat;
import io.flowforge.application.execution.TaskCompletion;
import io.flowforge.application.execution.TaskHeartbeatIngestion;
import io.flowforge.application.execution.TaskHeartbeatIngestionOutcome;
import io.flowforge.application.execution.TaskOutcome;
import io.flowforge.application.execution.TaskWorkItem;
import io.flowforge.application.execution.DurableTaskQueue;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.domain.execution.ExecutionEventType;
import io.flowforge.domain.execution.TaskRunStatus;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.TaskReliabilityPolicy;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false",
        "flowforge.leases.reaper-enabled=false",
        "flowforge.leases.duration=30s"
})
@Testcontainers(disabledWithoutDocker = true)
class WorkerLeaseRecoveryIntegrationTest {
    private static final Instant TIME = Instant.parse("2026-09-05T08:00:00Z");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired WorkflowService workflows;
    @Autowired ExecutionRepository executions;
    @Autowired DurableTaskQueue queue;
    @Autowired AttemptLeaseRecovery leaseRecovery;
    @Autowired TaskHeartbeatIngestion heartbeats;
    @Autowired JdbcClient jdbc;

    @BeforeEach
    void removeExecutions() {
        jdbc.sql("DELETE FROM workflow_execution").update();
        jdbc.sql("DELETE FROM coordination_permit WHERE resource_key LIKE 'concurrency:%'").update();
        jdbc.sql("DELETE FROM coordination_resource WHERE resource_key LIKE 'concurrency:%'").update();
    }

    @Test
    void issuesTokenAndRenewsLeaseIdempotentlyWhileRejectingAStaleWorker() {
        TaskWorkItem work = startAndClaim(1, "heartbeat");
        assertThat(work.fencingToken()).isNotNull();
        assertThat(leaseDeadline(work.taskRunId())).isEqualTo(TIME.plusSeconds(31));

        UUID eventId = UUID.randomUUID();
        InboundTaskHeartbeat heartbeat = heartbeat(eventId, work, work.fencingToken());
        assertThat(heartbeats.ingest(heartbeat, "{\"event\":\"one\"}", TIME.plusSeconds(20)))
                .isEqualTo(TaskHeartbeatIngestionOutcome.APPLIED);
        assertThat(leaseDeadline(work.taskRunId())).isEqualTo(TIME.plusSeconds(50));
        assertThat(heartbeats.ingest(heartbeat, "{\"event\":\"one\"}", TIME.plusSeconds(21)))
                .isEqualTo(TaskHeartbeatIngestionOutcome.DUPLICATE);

        InboundTaskHeartbeat stale = heartbeat(UUID.randomUUID(), work, UUID.randomUUID());
        assertThat(heartbeats.ingest(stale, "{\"event\":\"stale\"}", TIME.plusSeconds(25)))
                .isEqualTo(TaskHeartbeatIngestionOutcome.STALE);
        assertThat(leaseDeadline(work.taskRunId())).isEqualTo(TIME.plusSeconds(50));
    }

    @Test
    void recoversAnOrphanThroughRetryAndFencesTheDelayedOldWorker() {
        TaskWorkItem first = startAndClaim(2, "orphan");

        assertThat(leaseRecovery.reapExpiredLeases(10, TIME.plusSeconds(30))).isZero();
        assertThat(leaseRecovery.reapExpiredLeases(10, TIME.plusSeconds(31))).isEqualTo(1);
        assertThat(executions.findById(first.workflowRunId()).orElseThrow().events())
                .extracting(event -> event.type())
                .contains(ExecutionEventType.TASK_LEASE_EXPIRED, ExecutionEventType.TASK_RETRY_SCHEDULED);
        assertThat(activeTaskPermits()).isZero();

        assertThat(queue.releaseDueRetries(10, TIME.plusSeconds(31))).isEqualTo(1);
        assertThat(queue.enqueueReadyTasks(first.workflowRunId(), 10, TIME.plusSeconds(31))).isEqualTo(1);
        TaskWorkItem second = distributedWorkItem(first.workflowRunId(), 2);
        assertThat(second.attemptNumber()).isEqualTo(2);
        assertThat(second.fencingToken()).isNotEqualTo(first.fencingToken());

        assertThatThrownBy(() -> executions.completeTask(
                new TaskCompletion(
                        second.taskRunId(), second.stateVersion(), TaskOutcome.SUCCEEDED,
                        null, null, null, first.fencingToken()
                ),
                TIME.plusSeconds(32)
        )).isInstanceOf(ExecutionConflictException.class).hasMessageContaining("fencing token");

        executions.completeTask(
                new TaskCompletion(
                        second.taskRunId(), second.stateVersion(), TaskOutcome.SUCCEEDED,
                        null, null, null, second.fencingToken()
                ),
                TIME.plusSeconds(32)
        );
        assertThat(executions.findById(first.workflowRunId()).orElseThrow().tasks())
                .singleElement().extracting(task -> task.status()).isEqualTo(TaskRunStatus.SUCCEEDED);
    }

    @Test
    void competingReplicasRecoverOneExpiredLeaseExactlyOnce() throws Exception {
        TaskWorkItem work = startAndClaim(1, "lease-race");
        CountDownLatch start = new CountDownLatch(1);
        int first;
        int second;
        try (var executor = Executors.newFixedThreadPool(2)) {
            var one = executor.submit(() -> {
                start.await();
                return leaseRecovery.reapExpiredLeases(10, TIME.plusSeconds(31));
            });
            var two = executor.submit(() -> {
                start.await();
                return leaseRecovery.reapExpiredLeases(10, TIME.plusSeconds(31));
            });
            start.countDown();
            first = one.get(10, TimeUnit.SECONDS);
            second = two.get(10, TimeUnit.SECONDS);
        }
        assertThat(first + second).isEqualTo(1);
        assertThat(executions.findById(work.workflowRunId()).orElseThrow().events())
                .filteredOn(event -> event.type() == ExecutionEventType.TASK_LEASE_EXPIRED)
                .hasSize(1);
    }

    private TaskWorkItem startAndClaim(int maxAttempts, String idempotencyKey) {
        TaskReliabilityPolicy policy = new TaskReliabilityPolicy(
                maxAttempts, Duration.ZERO, 2.0, Duration.ZERO, 0.0, null, Set.of()
        );
        WorkflowDefinition created = workflows.create(new WorkflowDraft(
                "Lease test", null,
                List.of(new TaskDefinition("ROOT", "Root", "NOOP", Map.of(), policy, 1)),
                List.of()
        ));
        WorkflowDefinition published = workflows.publish(created.id(), created.lockVersion());
        var started = executions.start(published.id(), idempotencyKey, TIME);
        assertThat(queue.enqueueReadyTasks(started.workflow().id(), 10, TIME.plusSeconds(1))).isEqualTo(1);
        return distributedWorkItem(started.workflow().id(), 1);
    }

    private TaskWorkItem distributedWorkItem(UUID workflowId, int attemptNumber) {
        var task = executions.findById(workflowId).orElseThrow().tasks().getFirst();
        UUID token = jdbc.sql("""
                SELECT fencing_token FROM task_attempt
                 WHERE task_execution_id = :taskId AND attempt_number = :attemptNumber
                """)
                .param("taskId", task.id())
                .param("attemptNumber", attemptNumber)
                .query(UUID.class).single();
        return new TaskWorkItem(
                workflowId, task.id(), task.taskKey(), "NOOP", Map.of(),
                task.stateVersion(), attemptNumber, token, null
        );
    }

    private InboundTaskHeartbeat heartbeat(UUID eventId, TaskWorkItem work, UUID fencingToken) {
        return new InboundTaskHeartbeat(
                eventId, work.workflowRunId(), work.taskRunId(), work.taskKey(),
                work.attemptNumber(), fencingToken, "worker-a"
        );
    }

    private Instant leaseDeadline(UUID taskId) {
        return jdbc.sql("SELECT lease_deadline FROM task_attempt WHERE task_execution_id = :taskId AND status = 'RUNNING'")
                .param("taskId", taskId)
                .query(Instant.class)
                .single();
    }

    private long activeTaskPermits() {
        return jdbc.sql("""
                SELECT COUNT(*) FROM coordination_permit
                 WHERE resource_key LIKE 'concurrency:task:%'
                   AND status = 'ACTIVE'
                """).query(Long.class).single();
    }
}
