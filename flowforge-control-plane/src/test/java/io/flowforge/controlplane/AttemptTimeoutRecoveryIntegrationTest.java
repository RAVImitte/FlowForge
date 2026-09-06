package io.flowforge.controlplane;

import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.application.execution.AttemptTimeoutRecovery;
import io.flowforge.application.execution.ExecutionConflictException;
import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.execution.DurableTaskQueue;
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
import io.micrometer.core.instrument.MeterRegistry;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
class AttemptTimeoutRecoveryIntegrationTest {
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
    AttemptTimeoutRecovery timeoutRecovery;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    MeterRegistry meters;

    @BeforeEach
    void removeExecutions() {
        jdbc.sql("DELETE FROM workflow_execution").update();
        jdbc.sql("DELETE FROM coordination_permit WHERE resource_key LIKE 'concurrency:%'").update();
        jdbc.sql("DELETE FROM coordination_resource WHERE resource_key LIKE 'concurrency:%'").update();
    }

    @Test
    void persistsDeadlineRetriesWhenDueAndRejectsTheLateFirstResult() {
        start(policy(2, Duration.ofSeconds(5), Duration.ofSeconds(10)), "timeout-retry");
        TaskWorkItem first = onlyClaim(TIME.plusSeconds(1));

        Instant storedDeadline = jdbc.sql("""
                        SELECT attempt_deadline
                          FROM task_attempt
                         WHERE task_execution_id = :taskId
                           AND status = 'RUNNING'
                        """)
                .param("taskId", first.taskRunId())
                .query(Instant.class)
                .single();
        assertThat(storedDeadline).isEqualTo(TIME.plusSeconds(11));
        assertThat(timeoutRecovery.reapTimedOutAttempts(10, TIME.plusSeconds(10))).isZero();

        assertThat(timeoutRecovery.reapTimedOutAttempts(10, TIME.plusSeconds(11))).isEqualTo(1);
        WorkflowExecution scheduled = executions.findById(TenantId.LOCAL, first.workflowRunId()).orElseThrow();
        assertThat(scheduled.workflow().status()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(scheduled.tasks()).singleElement().satisfies(task -> {
            assertThat(task.status()).isEqualTo(TaskRunStatus.RETRY_SCHEDULED);
            assertThat(task.nextAttemptAt()).isEqualTo(TIME.plusSeconds(16));
        });
        assertThat(scheduled.attempts()).singleElement()
                .extracting(attempt -> attempt.status())
                .isEqualTo(TaskAttemptStatus.TIMED_OUT);
        assertThat(scheduled.events()).extracting(event -> event.type())
                .contains(ExecutionEventType.TASK_ATTEMPT_TIMED_OUT,
                        ExecutionEventType.TASK_RETRY_SCHEDULED);
        assertThat(activeTaskPermits()).isZero();

        assertThatThrownBy(() -> executions.completeTask(
                TaskCompletion.from(first, TaskResult.succeeded()),
                TIME.plusSeconds(12)
        )).isInstanceOf(ExecutionConflictException.class);

        assertThat(queue.releaseDueRetries(10, TIME.plusSeconds(16))).isEqualTo(1);
        TaskWorkItem second = onlyClaim(TIME.plusSeconds(16));
        assertThat(second.attemptNumber()).isEqualTo(2);
        WorkflowExecution completed = executions.completeTask(
                TaskCompletion.from(second, TaskResult.succeeded()),
                TIME.plusSeconds(17)
        ).execution();
        assertThat(completed.workflow().status()).isEqualTo(WorkflowRunStatus.SUCCEEDED);
    }

    @Test
    void terminalTimeoutFailsTheTaskAndWorkflowAndPublishesMetricsAfterCommit() {
        double taskTimeoutsBefore = count("flowforge.timeouts.tasks");
        double terminalTimeoutsBefore = count("flowforge.timeouts.terminal");
        double workflowTimeoutsBefore = count("flowforge.timeouts.workflows.failed");
        start(policy(1, Duration.ZERO, Duration.ofSeconds(3)), "terminal-timeout");
        TaskWorkItem work = onlyClaim(TIME.plusSeconds(1));

        assertThat(timeoutRecovery.reapTimedOutAttempts(10, TIME.plusSeconds(4))).isEqualTo(1);

        WorkflowExecution timedOut = executions.findById(TenantId.LOCAL, work.workflowRunId()).orElseThrow();
        assertThat(timedOut.workflow().status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(timedOut.tasks()).singleElement()
                .extracting(task -> task.status())
                .isEqualTo(TaskRunStatus.TIMED_OUT);
        assertThat(timedOut.attempts()).singleElement()
                .extracting(attempt -> attempt.status())
                .isEqualTo(TaskAttemptStatus.TIMED_OUT);
        assertThat(timedOut.events()).extracting(event -> event.type())
                .contains(
                        ExecutionEventType.TASK_ATTEMPT_TIMED_OUT,
                        ExecutionEventType.TASK_TIMED_OUT,
                        ExecutionEventType.WORKFLOW_FAILED
                );
        assertThat(count("flowforge.timeouts.tasks") - taskTimeoutsBefore).isEqualTo(1.0);
        assertThat(count("flowforge.timeouts.terminal") - terminalTimeoutsBefore).isEqualTo(1.0);
        assertThat(count("flowforge.timeouts.workflows.failed") - workflowTimeoutsBefore).isEqualTo(1.0);
    }

    @Test
    void competingReapersTimeOutOneAttemptExactlyOnce() throws Exception {
        start(policy(1, Duration.ZERO, Duration.ofSeconds(1)), "timeout-race");
        TaskWorkItem work = onlyClaim(TIME.plusSeconds(1));

        CountDownLatch start = new CountDownLatch(1);
        int firstReaped;
        int secondReaped;
        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> {
                start.await();
                return timeoutRecovery.reapTimedOutAttempts(10, TIME.plusSeconds(2));
            });
            var second = executor.submit(() -> {
                start.await();
                return timeoutRecovery.reapTimedOutAttempts(10, TIME.plusSeconds(2));
            });
            start.countDown();
            firstReaped = first.get(10, TimeUnit.SECONDS);
            secondReaped = second.get(10, TimeUnit.SECONDS);
        }

        assertThat(firstReaped + secondReaped).isEqualTo(1);
        WorkflowExecution timedOut = executions.findById(TenantId.LOCAL, work.workflowRunId()).orElseThrow();
        assertThat(timedOut.attempts()).hasSize(1);
        assertThat(timedOut.events()).filteredOn(event ->
                event.type() == ExecutionEventType.TASK_ATTEMPT_TIMED_OUT).hasSize(1);
    }

    @Test
    void competingReapersClaimDisjointBatches() throws Exception {
        int workload = 12;
        int batchSize = workload / 2;
        for (int index = 0; index < workload; index++) {
            start(policy(1, Duration.ZERO, Duration.ofSeconds(1)), "timeout-batch-race-" + index);
        }
        assertThat(executions.claimReadyTasks(TenantId.LOCAL, workload, TIME.plusSeconds(1))).hasSize(workload);

        CountDownLatch start = new CountDownLatch(1);
        int firstReaped;
        int secondReaped;
        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> {
                start.await();
                return timeoutRecovery.reapTimedOutAttempts(batchSize, TIME.plusSeconds(2));
            });
            var second = executor.submit(() -> {
                start.await();
                return timeoutRecovery.reapTimedOutAttempts(batchSize, TIME.plusSeconds(2));
            });
            start.countDown();
            firstReaped = first.get(10, TimeUnit.SECONDS);
            secondReaped = second.get(10, TimeUnit.SECONDS);
        }

        assertThat(firstReaped).isEqualTo(batchSize);
        assertThat(secondReaped).isEqualTo(batchSize);
        assertThat(jdbc.sql("""
                SELECT COUNT(DISTINCT workflow_execution_id)
                  FROM execution_event
                 WHERE event_type = 'TASK_ATTEMPT_TIMED_OUT'
                """).query(Long.class).single()).isEqualTo((long) workload);
        assertThat(jdbc.sql("""
                SELECT COUNT(*)
                  FROM task_attempt
                 WHERE status = 'RUNNING'
                """).query(Long.class).single()).isZero();
    }

    private WorkflowExecution start(TaskReliabilityPolicy policy, String idempotencyKey) {
        WorkflowDefinition draft = workflows.create(TenantId.LOCAL, new WorkflowDraft(
                "Timeout test",
                null,
                List.of(new TaskDefinition("ROOT", "Root", "NOOP", Map.of(), policy, 1)),
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

    private double count(String name) {
        var counter = meters.find(name).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private long activeTaskPermits() {
        return jdbc.sql("""
                SELECT COUNT(*) FROM coordination_permit
                 WHERE resource_key LIKE 'concurrency:task:%'
                   AND status = 'ACTIVE'
                """).query(Long.class).single();
    }

    private static TaskReliabilityPolicy policy(
            int maxAttempts,
            Duration backoff,
            Duration timeout
    ) {
        return new TaskReliabilityPolicy(
                maxAttempts,
                backoff,
                2.0,
                maxAttempts == 1 ? Duration.ZERO : backoff.multipliedBy(4),
                0.0,
                timeout,
                Set.of()
        );
    }
}
