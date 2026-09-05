package io.flowforge.controlplane;

import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.execution.TaskCompletion;
import io.flowforge.application.execution.TaskCompletionResult;
import io.flowforge.application.execution.TaskResult;
import io.flowforge.application.execution.TaskWorkItem;
import io.flowforge.application.execution.WorkflowNotPublishedException;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.domain.execution.ExecutionEventType;
import io.flowforge.domain.execution.TaskAttemptStatus;
import io.flowforge.domain.execution.TaskRunStatus;
import io.flowforge.domain.execution.WorkflowExecution;
import io.flowforge.domain.execution.WorkflowRunStatus;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.TaskDependency;
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

import java.sql.Timestamp;
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
class WorkflowExecutionPersistenceIntegrationTest {
    private static final Instant TIME = Instant.parse("2026-09-03T12:00:00Z");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    WorkflowService workflowService;

    @Autowired
    ExecutionRepository executionRepository;

    @Autowired
    JdbcClient jdbc;

    @BeforeEach
    void removeExecutions() {
        jdbc.sql("DELETE FROM workflow_execution").update();
    }

    @Test
    void materializesTheLatestPublishedVersionIdempotently() {
        WorkflowDefinition published = publish(workflow(
                List.of(task("ROOT"), task("CHILD")),
                List.of(new TaskDependency("CHILD", "ROOT"))
        ));
        workflowService.update(
                published.id(),
                published.lockVersion(),
                workflow(List.of(task("NEW_DRAFT_TASK")), List.of())
        );

        WorkflowExecution first = executionRepository.start(published.id(), "request-1", TIME);
        WorkflowExecution duplicate = executionRepository.start(published.id(), "request-1", TIME.plusSeconds(1));

        assertThat(duplicate.workflow().id()).isEqualTo(first.workflow().id());
        assertThat(first.workflow().workflowVersion()).isEqualTo(1);
        assertThat(first.tasks()).extracting(task -> task.taskKey() + ":" + task.status())
                .containsExactly("ROOT:READY", "CHILD:BLOCKED");
    }

    @Test
    void rejectsExecutionOfAnUnpublishedWorkflow() {
        WorkflowDefinition draft = workflowService.create(workflow(List.of(task("ROOT")), List.of()));

        assertThatThrownBy(() -> executionRepository.start(draft.id(), "request-1", TIME))
                .isInstanceOf(WorkflowNotPublishedException.class);
    }

    @Test
    void materializesAttemptTimeoutIntoDurableAttemptState() {
        TaskReliabilityPolicy policy = new TaskReliabilityPolicy(
                3,
                Duration.ofSeconds(1),
                2.0,
                Duration.ofSeconds(10),
                0.0,
                Duration.ofSeconds(5),
                Set.of("TIMEOUT")
        );
        WorkflowDefinition published = publish(workflow(
                List.of(new TaskDefinition("ROOT", "ROOT", "NOOP", Map.of(), policy)),
                List.of()
        ));
        executionRepository.start(published.id(), "attempt-timeout", TIME);

        Instant claimedAt = TIME.plusSeconds(1);
        TaskWorkItem claimed = onlyClaim(claimedAt);
        Timestamp deadline = jdbc.sql("""
                SELECT attempt_deadline
                  FROM task_attempt
                 WHERE task_execution_id = :taskId
                """)
                .param("taskId", claimed.taskRunId())
                .query(Timestamp.class)
                .single();

        assertThat(claimed.attemptTimeoutMs()).isEqualTo(5_000L);
        assertThat(deadline.toInstant()).isEqualTo(claimedAt.plusSeconds(5));
    }

    @Test
    void resolvesFanOutAndFanInAndCompletesTheWorkflow() {
        WorkflowDefinition published = publish(workflow(
                List.of(task("ROOT"), task("LEFT"), task("RIGHT"), task("JOIN")),
                List.of(
                        new TaskDependency("LEFT", "ROOT"),
                        new TaskDependency("RIGHT", "ROOT"),
                        new TaskDependency("JOIN", "LEFT"),
                        new TaskDependency("JOIN", "RIGHT")
                )
        ));
        WorkflowExecution execution = executionRepository.start(published.id(), "fan-in", TIME);

        TaskWorkItem root = onlyClaim(TIME.plusSeconds(1));
        executionRepository.completeTask(TaskCompletion.from(root, TaskResult.succeeded()), TIME.plusSeconds(2));

        List<TaskWorkItem> branches = executionRepository.claimReadyTasks(10, TIME.plusSeconds(3));
        assertThat(branches).extracting(TaskWorkItem::taskKey)
                .containsExactlyInAnyOrder("LEFT", "RIGHT");
        TaskWorkItem left = byKey(branches, "LEFT");
        TaskWorkItem right = byKey(branches, "RIGHT");

        executionRepository.completeTask(TaskCompletion.from(left, TaskResult.succeeded()), TIME.plusSeconds(4));
        assertThat(executionRepository.claimReadyTasks(10, TIME.plusSeconds(5))).isEmpty();

        executionRepository.completeTask(TaskCompletion.from(right, TaskResult.succeeded()), TIME.plusSeconds(6));
        TaskWorkItem join = onlyClaim(TIME.plusSeconds(7));
        assertThat(join.taskKey()).isEqualTo("JOIN");

        WorkflowExecution completed = executionRepository.completeTask(
                TaskCompletion.from(join, TaskResult.succeeded()),
                TIME.plusSeconds(8)
        ).execution();

        assertThat(completed.workflow().id()).isEqualTo(execution.workflow().id());
        assertThat(completed.workflow().status()).isEqualTo(WorkflowRunStatus.SUCCEEDED);
        assertThat(completed.tasks()).extracting(task -> task.status())
                .containsOnly(TaskRunStatus.SUCCEEDED);
    }

    @Test
    void failsTheWorkflowAndCancelsQueuedDependents() {
        WorkflowDefinition published = publish(workflow(
                List.of(task("ROOT"), task("CHILD")),
                List.of(new TaskDependency("CHILD", "ROOT"))
        ));
        executionRepository.start(published.id(), "failure", TIME);
        TaskWorkItem root = onlyClaim(TIME.plusSeconds(1));

        WorkflowExecution failed = executionRepository.completeTask(
                TaskCompletion.from(root, TaskResult.failed("TEST_FAILURE", "Expected failure")),
                TIME.plusSeconds(2)
        ).execution();

        assertThat(failed.workflow().status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(failed.tasks()).filteredOn(task -> task.taskKey().equals("CHILD"))
                .extracting(task -> task.status())
                .containsExactly(TaskRunStatus.CANCELLED);
        assertThat(failed.attempts()).singleElement()
                .satisfies(attempt -> {
                    assertThat(attempt.status()).isEqualTo(TaskAttemptStatus.FAILED);
                    assertThat(attempt.errorCode()).isEqualTo("TEST_FAILURE");
                });
    }

    @Test
    void cancellationWaitsForRunningTasksAndIsIdempotent() {
        WorkflowDefinition published = publish(workflow(
                List.of(task("ROOT"), task("CHILD")),
                List.of(new TaskDependency("CHILD", "ROOT"))
        ));
        WorkflowExecution started = executionRepository.start(published.id(), "cancel", TIME);
        TaskWorkItem root = onlyClaim(TIME.plusSeconds(1));

        WorkflowExecution cancelling = executionRepository.cancel(started.workflow().id(), TIME.plusSeconds(2));
        assertThat(cancelling.workflow().status()).isEqualTo(WorkflowRunStatus.CANCELLING);
        assertThat(cancelling.tasks()).filteredOn(task -> task.taskKey().equals("CHILD"))
                .extracting(task -> task.status())
                .containsExactly(TaskRunStatus.CANCELLED);

        WorkflowExecution cancelled = executionRepository.completeTask(
                TaskCompletion.from(root, TaskResult.succeeded()),
                TIME.plusSeconds(3)
        ).execution();
        WorkflowExecution duplicateCancel = executionRepository.cancel(
                started.workflow().id(),
                TIME.plusSeconds(4)
        );

        assertThat(cancelled.workflow().status()).isEqualTo(WorkflowRunStatus.CANCELLED);
        assertThat(duplicateCancel.workflow().stateVersion())
                .isEqualTo(cancelled.workflow().stateVersion());
    }

    @Test
    void simultaneousAndDuplicateCompletionsAdvanceTheDagExactlyOnce() throws Exception {
        WorkflowDefinition published = publish(workflow(
                List.of(task("ROOT"), task("LEFT"), task("RIGHT"), task("JOIN")),
                List.of(
                        new TaskDependency("LEFT", "ROOT"),
                        new TaskDependency("RIGHT", "ROOT"),
                        new TaskDependency("JOIN", "LEFT"),
                        new TaskDependency("JOIN", "RIGHT")
                )
        ));
        WorkflowExecution execution = executionRepository.start(published.id(), "concurrency", TIME);
        TaskWorkItem root = onlyClaim(TIME.plusSeconds(1));
        executionRepository.completeTask(TaskCompletion.from(root, TaskResult.succeeded()), TIME.plusSeconds(2));
        List<TaskWorkItem> branches = executionRepository.claimReadyTasks(10, TIME.plusSeconds(3));
        TaskWorkItem left = byKey(branches, "LEFT");
        TaskWorkItem right = byKey(branches, "RIGHT");

        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var leftCompletion = executor.submit(() -> {
                start.await();
                return executionRepository.completeTask(
                        TaskCompletion.from(left, TaskResult.succeeded()),
                        TIME.plusSeconds(4)
                );
            });
            var rightCompletion = executor.submit(() -> {
                start.await();
                return executionRepository.completeTask(
                        TaskCompletion.from(right, TaskResult.succeeded()),
                        TIME.plusSeconds(4)
                );
            });
            start.countDown();
            leftCompletion.get(10, TimeUnit.SECONDS);
            rightCompletion.get(10, TimeUnit.SECONDS);
        }

        TaskCompletionResult duplicate = executionRepository.completeTask(
                TaskCompletion.from(left, TaskResult.succeeded()),
                TIME.plusSeconds(5)
        );
        WorkflowExecution current = executionRepository.findById(execution.workflow().id()).orElseThrow();
        var join = current.tasks().stream().filter(task -> task.taskKey().equals("JOIN")).findFirst().orElseThrow();
        long joinReadyEvents = current.events().stream()
                .filter(event -> event.taskRunId() != null && event.taskRunId().equals(join.id()))
                .filter(event -> event.type() == ExecutionEventType.TASK_READY)
                .count();

        assertThat(duplicate.applied()).isFalse();
        assertThat(join.status()).isEqualTo(TaskRunStatus.READY);
        assertThat(joinReadyEvents).isEqualTo(1);
        assertThat(current.tasks()).filteredOn(task -> Set.of("LEFT", "RIGHT").contains(task.taskKey()))
                .extracting(task -> task.status())
                .containsOnly(TaskRunStatus.SUCCEEDED);
    }

    private TaskWorkItem onlyClaim(Instant time) {
        List<TaskWorkItem> claimed = executionRepository.claimReadyTasks(10, time);
        assertThat(claimed).hasSize(1);
        return claimed.getFirst();
    }

    private static TaskWorkItem byKey(List<TaskWorkItem> items, String key) {
        return items.stream().filter(item -> item.taskKey().equals(key)).findFirst().orElseThrow();
    }

    private WorkflowDefinition publish(WorkflowDraft draft) {
        WorkflowDefinition created = workflowService.create(draft);
        return workflowService.publish(created.id(), created.lockVersion());
    }

    private static WorkflowDraft workflow(List<TaskDefinition> tasks, List<TaskDependency> dependencies) {
        return new WorkflowDraft("Execution test", null, tasks, dependencies);
    }

    private static TaskDefinition task(String key) {
        return new TaskDefinition(key, key, "NOOP", Map.of());
    }
}
