package io.flowforge.controlplane;

import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.application.execution.DurableTaskQueue;
import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.execution.TaskCompletion;
import io.flowforge.application.execution.TaskResult;
import io.flowforge.application.execution.TaskWorkItem;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.domain.execution.ExecutionEventType;
import io.flowforge.domain.execution.TaskAttemptStatus;
import io.flowforge.domain.execution.TaskRunStatus;
import io.flowforge.domain.execution.WorkflowExecution;
import io.flowforge.domain.execution.WorkflowRunStatus;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
class RetrySchedulingPersistenceIntegrationTest {
    private static final Instant TIME = Instant.parse("2026-09-04T12:00:00Z");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    WorkflowService workflows;

    @Autowired
    ExecutionRepository executions;

    @Autowired
    DurableTaskQueue queue;

    @Autowired
    JdbcClient jdbc;

    @BeforeEach
    void removeExecutions() {
        jdbc.sql("DELETE FROM workflow_execution").update();
    }

    @Test
    void persistsDelayedRetryThenExhaustsWithoutFailingWorkflowEarly() {
        WorkflowExecution execution = start(retryPolicy(2, Duration.ofSeconds(10)), "delayed-retry");
        TaskWorkItem first = onlyClaim(TIME.plusSeconds(1));

        WorkflowExecution scheduled = executions.completeTask(
                TaskCompletion.from(first, TaskResult.retryableFailure("TRANSIENT", "try again")),
                TIME.plusSeconds(2)
        ).execution();

        assertThat(scheduled.workflow().status()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(scheduled.tasks()).singleElement().satisfies(task -> {
            assertThat(task.status()).isEqualTo(TaskRunStatus.RETRY_SCHEDULED);
            assertThat(task.nextAttemptAt()).isEqualTo(TIME.plusSeconds(12));
        });
        assertThat(scheduled.attempts()).singleElement()
                .extracting(attempt -> attempt.status())
                .isEqualTo(TaskAttemptStatus.FAILED);
        assertThat(queue.releaseDueRetries(10, TIME.plusSeconds(11))).isZero();
        assertThat(executions.claimReadyTasks(TenantId.LOCAL, 10, TIME.plusSeconds(11))).isEmpty();

        assertThat(queue.releaseDueRetries(10, TIME.plusSeconds(12))).isEqualTo(1);
        TaskWorkItem second = onlyClaim(TIME.plusSeconds(12));
        assertThat(second.attemptNumber()).isEqualTo(2);

        WorkflowExecution exhausted = executions.completeTask(
                TaskCompletion.from(second, TaskResult.retryableFailure("TRANSIENT", "still unavailable")),
                TIME.plusSeconds(13)
        ).execution();

        assertThat(exhausted.workflow().status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(exhausted.tasks()).singleElement()
                .extracting(task -> task.status())
                .isEqualTo(TaskRunStatus.FAILED);
        assertThat(exhausted.attempts()).hasSize(2)
                .extracting(attempt -> attempt.status())
                .containsOnly(TaskAttemptStatus.FAILED);
        assertThat(exhausted.events()).extracting(event -> event.type())
                .contains(
                        ExecutionEventType.TASK_RETRY_SCHEDULED,
                        ExecutionEventType.TASK_RETRY_READY,
                        ExecutionEventType.TASK_RETRY_STARTED,
                        ExecutionEventType.TASK_RETRY_EXHAUSTED,
                        ExecutionEventType.TASK_DEAD_LETTERED,
                        ExecutionEventType.WORKFLOW_FAILED
                );
        assertThat(jdbc.sql("""
                SELECT COUNT(*) FROM control_plane_outbox
                 WHERE workflow_execution_id = :executionId
                   AND message_kind = 'EXECUTION_EVENT'
                   AND payload -> 'payload' ->> 'transitionType' = 'TASK_DEAD_LETTERED'
                """)
                .param("executionId", exhausted.workflow().id())
                .query(Long.class)
                .single()).isEqualTo(1);
        assertThat(execution.workflow().id()).isEqualTo(exhausted.workflow().id());
    }

    @Test
    void workerClassificationControlsRetryWhenPolicyHasNoAllowList() {
        TaskReliabilityPolicy policy = new TaskReliabilityPolicy(
                3,
                Duration.ZERO,
                2.0,
                Duration.ZERO,
                0.0,
                null,
                Set.of()
        );
        start(policy, "worker-classification");
        TaskWorkItem first = onlyClaim(TIME.plusSeconds(1));

        WorkflowExecution failed = executions.completeTask(
                TaskCompletion.from(first, TaskResult.failed("BAD_REQUEST", "do not retry")),
                TIME.plusSeconds(2)
        ).execution();

        assertThat(failed.workflow().status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(failed.attempts()).hasSize(1);
        assertThat(failed.events()).extracting(event -> event.type())
                .doesNotContain(ExecutionEventType.TASK_RETRY_SCHEDULED);
    }

    @Test
    void competingSchedulersReleaseOneRetryExactlyOnce() throws Exception {
        start(retryPolicy(2, Duration.ZERO), "scheduler-race");
        TaskWorkItem first = onlyClaim(TIME.plusSeconds(1));
        executions.completeTask(
                TaskCompletion.from(first, TaskResult.retryableFailure("TRANSIENT", "retry")),
                TIME.plusSeconds(2)
        );

        CountDownLatch start = new CountDownLatch(1);
        int firstReleased;
        int secondReleased;
        try (var executor = Executors.newFixedThreadPool(2)) {
            var firstScheduler = executor.submit(() -> {
                start.await();
                return queue.releaseDueRetries(10, TIME.plusSeconds(2));
            });
            var secondScheduler = executor.submit(() -> {
                start.await();
                return queue.releaseDueRetries(10, TIME.plusSeconds(2));
            });
            start.countDown();
            firstReleased = firstScheduler.get(10, TimeUnit.SECONDS);
            secondReleased = secondScheduler.get(10, TimeUnit.SECONDS);
        }

        assertThat(firstReleased + secondReleased).isEqualTo(1);
        TaskWorkItem secondAttempt = onlyClaim(TIME.plusSeconds(3));
        assertThat(secondAttempt.attemptNumber()).isEqualTo(2);
        assertThat(executions.claimReadyTasks(TenantId.LOCAL, 10, TIME.plusSeconds(3))).isEmpty();
    }

    @Test
    void competingSchedulersClaimDisjointBatches() throws Exception {
        int workload = 12;
        int batchSize = workload / 2;
        for (int index = 0; index < workload; index++) {
            start(retryPolicy(2, Duration.ZERO), "scheduler-batch-race-" + index);
        }
        List<TaskWorkItem> firstAttempts = executions.claimReadyTasks(
                TenantId.LOCAL, workload, TIME.plusSeconds(1)
        );
        assertThat(firstAttempts).hasSize(workload);
        firstAttempts.forEach(work -> executions.completeTask(
                TaskCompletion.from(work, TaskResult.retryableFailure("TRANSIENT", "retry")),
                TIME.plusSeconds(2)
        ));

        CountDownLatch start = new CountDownLatch(1);
        int firstReleased;
        int secondReleased;
        try (var executor = Executors.newFixedThreadPool(2)) {
            var firstScheduler = executor.submit(() -> {
                start.await();
                return queue.releaseDueRetries(batchSize, TIME.plusSeconds(2));
            });
            var secondScheduler = executor.submit(() -> {
                start.await();
                return queue.releaseDueRetries(batchSize, TIME.plusSeconds(2));
            });
            start.countDown();
            firstReleased = firstScheduler.get(10, TimeUnit.SECONDS);
            secondReleased = secondScheduler.get(10, TimeUnit.SECONDS);
        }

        assertThat(firstReleased).isEqualTo(batchSize);
        assertThat(secondReleased).isEqualTo(batchSize);
        assertThat(executions.claimReadyTasks(TenantId.LOCAL, workload, TIME.plusSeconds(3)))
                .hasSize(workload)
                .allSatisfy(work -> assertThat(work.attemptNumber()).isEqualTo(2));
        assertThat(jdbc.sql("""
                SELECT COUNT(DISTINCT workflow_execution_id)
                  FROM execution_event
                 WHERE event_type = 'TASK_RETRY_READY'
                """).query(Long.class).single()).isEqualTo((long) workload);
    }

    private WorkflowExecution start(TaskReliabilityPolicy policy, String idempotencyKey) {
        WorkflowDefinition draft = workflows.create(TenantId.LOCAL, new WorkflowDraft(
                "Retry test",
                null,
                List.of(new TaskDefinition("ROOT", "Root", "NOOP", Map.of(), policy)),
                List.of()
        ));
        WorkflowDefinition published = workflows.publish(TenantId.LOCAL, draft.id(), draft.lockVersion());
        return executions.start(TenantId.LOCAL, published.id(), idempotencyKey, TIME);
    }

    private TaskWorkItem onlyClaim(Instant now) {
        List<TaskWorkItem> claimed = executions.claimReadyTasks(TenantId.LOCAL, 10, now);
        assertThat(claimed).hasSize(1);
        return claimed.getFirst();
    }

    private static TaskReliabilityPolicy retryPolicy(int maxAttempts, Duration backoff) {
        return new TaskReliabilityPolicy(
                maxAttempts,
                backoff,
                2.0,
                backoff.multipliedBy(4),
                0.0,
                null,
                Set.of("TRANSIENT")
        );
    }
}
