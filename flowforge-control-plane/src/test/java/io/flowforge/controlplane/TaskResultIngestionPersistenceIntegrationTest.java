package io.flowforge.controlplane;

import io.flowforge.application.execution.DurableTaskQueue;
import io.flowforge.application.execution.ExecutionConflictException;
import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.execution.InboundTaskResult;
import io.flowforge.application.execution.TaskOutcome;
import io.flowforge.application.execution.TaskResultIngestion;
import io.flowforge.application.execution.TaskResultIngestionOutcome;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.domain.execution.TaskRun;
import io.flowforge.domain.execution.TaskRunStatus;
import io.flowforge.domain.execution.WorkflowExecution;
import io.flowforge.domain.execution.WorkflowRunStatus;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.TaskDependency;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskResultOutcomeV1;
import io.flowforge.messaging.TaskResultV1;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

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
        "flowforge.outbox.publisher-enabled=false",
        "flowforge.outbox.command-dispatch-enabled=false",
        "flowforge.results.consumer-enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
class TaskResultIngestionPersistenceIntegrationTest {
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
    DurableTaskQueue taskQueue;

    @Autowired
    TaskResultIngestion ingestion;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void removeExecutions() {
        jdbc.sql("DELETE FROM workflow_execution").update();
    }

    @Test
    void appliesFanOutAndFanInResultsAndEnqueuesEachReadyLevelAtomically() throws Exception {
        WorkflowExecution execution = start(
                List.of(task("ROOT"), task("LEFT"), task("RIGHT"), task("JOIN")),
                List.of(
                        new TaskDependency("LEFT", "ROOT"),
                        new TaskDependency("RIGHT", "ROOT"),
                        new TaskDependency("JOIN", "LEFT"),
                        new TaskDependency("JOIN", "RIGHT")
                )
        );
        assertThat(taskQueue.enqueueReadyTasks(execution.workflow().id(), 1000, TIME.plusSeconds(1)))
                .isEqualTo(1);

        UUID rootEvent = UUID.randomUUID();
        ResultMessage root = result(execution.workflow().id(), "ROOT", rootEvent, TaskOutcome.SUCCEEDED);
        assertThat(ingestion.ingest(root.result(), root.json(), TIME.plusSeconds(2)))
                .isEqualTo(TaskResultIngestionOutcome.APPLIED);
        assertThat(ingestion.ingest(root.result(), root.json(), TIME.plusSeconds(3)))
                .isEqualTo(TaskResultIngestionOutcome.DUPLICATE);

        WorkflowExecution afterRoot = current(execution.workflow().id());
        assertThat(afterRoot.tasks()).filteredOn(task -> List.of("LEFT", "RIGHT").contains(task.taskKey()))
                .extracting(TaskRun::status)
                .containsOnly(TaskRunStatus.RUNNING);
        assertThat(commandCount(execution.workflow().id())).isEqualTo(3);

        ResultMessage left = result(execution.workflow().id(), "LEFT", UUID.randomUUID(), TaskOutcome.SUCCEEDED);
        ingestion.ingest(left.result(), left.json(), TIME.plusSeconds(4));
        assertThat(task(current(execution.workflow().id()), "JOIN").status()).isEqualTo(TaskRunStatus.BLOCKED);

        ResultMessage right = result(execution.workflow().id(), "RIGHT", UUID.randomUUID(), TaskOutcome.SUCCEEDED);
        ingestion.ingest(right.result(), right.json(), TIME.plusSeconds(5));
        assertThat(task(current(execution.workflow().id()), "JOIN").status()).isEqualTo(TaskRunStatus.RUNNING);
        assertThat(commandCount(execution.workflow().id())).isEqualTo(4);

        ResultMessage join = result(execution.workflow().id(), "JOIN", UUID.randomUUID(), TaskOutcome.SUCCEEDED);
        ingestion.ingest(join.result(), join.json(), TIME.plusSeconds(6));
        WorkflowExecution completed = current(execution.workflow().id());
        assertThat(completed.workflow().status()).isEqualTo(WorkflowRunStatus.SUCCEEDED);
        assertThat(completed.tasks()).extracting(TaskRun::status).containsOnly(TaskRunStatus.SUCCEEDED);
        assertThat(inboxCount()).isEqualTo(4);
    }

    @Test
    void rollsBackInboxAndStateForStaleOrConflictingResults() throws Exception {
        WorkflowExecution execution = start(List.of(task("ROOT")), List.of());
        taskQueue.enqueueReadyTasks(execution.workflow().id(), 1000, TIME.plusSeconds(1));
        TaskRun root = task(current(execution.workflow().id()), "ROOT");
        UUID staleEventId = UUID.randomUUID();
        InboundTaskResult stale = new InboundTaskResult(
                staleEventId,
                execution.workflow().id(),
                root.id(),
                root.taskKey(),
                root.stateVersion() + 1,
                1,
                TaskOutcome.SUCCEEDED,
                null,
                null,
                null,
                fencingToken(root.id(), 1)
        );
        String staleJson = envelopeJson(stale);

        assertThatThrownBy(() -> ingestion.ingest(stale, staleJson, TIME.plusSeconds(2)))
                .isInstanceOf(ExecutionConflictException.class)
                .hasMessageContaining("state version");
        assertThat(inboxCount()).isZero();
        assertThat(task(current(execution.workflow().id()), "ROOT").status()).isEqualTo(TaskRunStatus.RUNNING);

        ResultMessage accepted = result(
                execution.workflow().id(), "ROOT", UUID.randomUUID(), TaskOutcome.SUCCEEDED
        );
        ingestion.ingest(accepted.result(), accepted.json(), TIME.plusSeconds(3));
        InboundTaskResult reusedId = new InboundTaskResult(
                accepted.result().eventId(),
                accepted.result().workflowExecutionId(),
                accepted.result().taskExecutionId(),
                accepted.result().taskKey(),
                accepted.result().expectedStateVersion(),
                accepted.result().attemptNumber(),
                TaskOutcome.FAILED,
                "CONFLICT",
                "different result",
                null,
                accepted.result().fencingToken()
        );
        assertThatThrownBy(() -> ingestion.ingest(
                reusedId, envelopeJson(reusedId), TIME.plusSeconds(4)
        )).isInstanceOf(ExecutionConflictException.class)
                .hasMessageContaining("reused with a different payload");
        assertThat(inboxCount()).isEqualTo(1);
    }

    @Test
    void failedResultFailsWorkflowAndNeverEnqueuesDependents() throws Exception {
        WorkflowExecution execution = start(
                List.of(task("ROOT"), task("CHILD")),
                List.of(new TaskDependency("CHILD", "ROOT"))
        );
        taskQueue.enqueueReadyTasks(execution.workflow().id(), 1000, TIME.plusSeconds(1));

        ResultMessage failure = result(execution.workflow().id(), "ROOT", UUID.randomUUID(), TaskOutcome.FAILED);
        ingestion.ingest(failure.result(), failure.json(), TIME.plusSeconds(2));

        WorkflowExecution failed = current(execution.workflow().id());
        assertThat(failed.workflow().status()).isEqualTo(WorkflowRunStatus.FAILED);
        assertThat(task(failed, "CHILD").status()).isEqualTo(TaskRunStatus.CANCELLED);
        assertThat(commandCount(execution.workflow().id())).isEqualTo(1);
    }

    @Test
    void completionDuringCancellationFinishesCancelledWithoutReleasingDependents() throws Exception {
        WorkflowExecution execution = start(
                List.of(task("ROOT"), task("CHILD")),
                List.of(new TaskDependency("CHILD", "ROOT"))
        );
        taskQueue.enqueueReadyTasks(execution.workflow().id(), 1000, TIME.plusSeconds(1));
        executions.cancel(execution.workflow().id(), TIME.plusSeconds(2));

        ResultMessage result = result(execution.workflow().id(), "ROOT", UUID.randomUUID(), TaskOutcome.SUCCEEDED);
        ingestion.ingest(result.result(), result.json(), TIME.plusSeconds(3));

        WorkflowExecution cancelled = current(execution.workflow().id());
        assertThat(cancelled.workflow().status()).isEqualTo(WorkflowRunStatus.CANCELLED);
        assertThat(task(cancelled, "CHILD").status()).isEqualTo(TaskRunStatus.CANCELLED);
        assertThat(commandCount(execution.workflow().id())).isEqualTo(1);
    }

    @RepeatedTest(5)
    void concurrentEquivalentResultsAdvanceTheDagOnlyOnce() throws Exception {
        WorkflowExecution execution = start(
                List.of(task("ROOT"), task("CHILD")),
                List.of(new TaskDependency("CHILD", "ROOT"))
        );
        taskQueue.enqueueReadyTasks(execution.workflow().id(), 1000, TIME.plusSeconds(1));
        ResultMessage first = result(
                execution.workflow().id(), "ROOT", UUID.randomUUID(), TaskOutcome.SUCCEEDED
        );
        ResultMessage second = result(
                execution.workflow().id(), "ROOT", UUID.randomUUID(), TaskOutcome.SUCCEEDED
        );
        CountDownLatch start = new CountDownLatch(1);
        Set<TaskResultIngestionOutcome> outcomes;
        try (var executor = Executors.newFixedThreadPool(2)) {
            var firstResult = executor.submit(() -> {
                start.await();
                return ingestion.ingest(first.result(), first.json(), TIME.plusSeconds(2));
            });
            var secondResult = executor.submit(() -> {
                start.await();
                return ingestion.ingest(second.result(), second.json(), TIME.plusSeconds(2));
            });
            start.countDown();
            outcomes = Set.of(
                    firstResult.get(10, TimeUnit.SECONDS),
                    secondResult.get(10, TimeUnit.SECONDS)
            );
        }

        assertThat(outcomes).containsExactlyInAnyOrder(
                TaskResultIngestionOutcome.APPLIED,
                TaskResultIngestionOutcome.REDUNDANT
        );
        assertThat(task(current(execution.workflow().id()), "CHILD").status())
                .isEqualTo(TaskRunStatus.RUNNING);
        assertThat(commandCount(execution.workflow().id())).isEqualTo(2);
        assertThat(inboxCount()).isEqualTo(2);
    }

    private WorkflowExecution start(List<TaskDefinition> tasks, List<TaskDependency> dependencies) {
        WorkflowDefinition created = workflows.create(new WorkflowDraft(
                "Result ingestion test", null, tasks, dependencies
        ));
        WorkflowDefinition published = workflows.publish(created.id(), created.lockVersion());
        return executions.start(published.id(), "result-" + UUID.randomUUID(), TIME);
    }

    private ResultMessage result(
            UUID workflowExecutionId,
            String taskKey,
            UUID eventId,
            TaskOutcome outcome
    ) throws Exception {
        TaskRun task = task(current(workflowExecutionId), taskKey);
        InboundTaskResult result = new InboundTaskResult(
                eventId,
                workflowExecutionId,
                task.id(),
                task.taskKey(),
                task.stateVersion(),
                1,
                outcome,
                outcome == TaskOutcome.FAILED ? "TEST_FAILURE" : null,
                outcome == TaskOutcome.FAILED ? "Expected failure" : null,
                null,
                fencingToken(task.id(), 1)
        );
        return new ResultMessage(result, envelopeJson(result));
    }

    private String envelopeJson(InboundTaskResult result) throws Exception {
        TaskResultV1 payload = new TaskResultV1(
                result.workflowExecutionId(),
                result.taskExecutionId(),
                result.taskKey(),
                result.expectedStateVersion(),
                result.attemptNumber(),
                TaskResultOutcomeV1.valueOf(result.outcome().name()),
                result.errorCode(),
                result.errorMessage(),
                result.fencingToken(),
                result.retryable()
        );
        return objectMapper.writeValueAsString(new MessageEnvelope<>(
                result.eventId(),
                TaskResultV1.EVENT_TYPE,
                TaskResultV1.SCHEMA_VERSION,
                TIME,
                result.workflowExecutionId(),
                payload
        ));
    }

    private WorkflowExecution current(UUID workflowExecutionId) {
        return executions.findById(workflowExecutionId).orElseThrow();
    }

    private UUID fencingToken(UUID taskId, int attemptNumber) {
        return jdbc.sql("""
                SELECT fencing_token FROM task_attempt
                 WHERE task_execution_id = :taskId AND attempt_number = :attemptNumber
                """)
                .param("taskId", taskId)
                .param("attemptNumber", attemptNumber)
                .query(UUID.class)
                .single();
    }

    private static TaskRun task(WorkflowExecution execution, String key) {
        return execution.tasks().stream().filter(task -> task.taskKey().equals(key)).findFirst().orElseThrow();
    }

    private long inboxCount() {
        return jdbc.sql("SELECT COUNT(*) FROM control_plane_result_inbox").query(Long.class).single();
    }

    private long commandCount(UUID workflowExecutionId) {
        return jdbc.sql("""
                SELECT COUNT(*) FROM control_plane_outbox
                 WHERE workflow_execution_id = :workflowExecutionId
                   AND message_kind = 'TASK_COMMAND'
                """).param("workflowExecutionId", workflowExecutionId).query(Long.class).single();
    }

    private static TaskDefinition task(String key) {
        return new TaskDefinition(key, key, "NOOP", Map.of());
    }

    private record ResultMessage(InboundTaskResult result, String json) {
    }
}
