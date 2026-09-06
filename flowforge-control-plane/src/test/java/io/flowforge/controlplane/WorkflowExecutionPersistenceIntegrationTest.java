package io.flowforge.controlplane;

import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.execution.DurableTaskQueue;
import io.flowforge.application.execution.ExecutionNotFoundException;
import io.flowforge.application.execution.AdmissionOverloadedException;
import io.flowforge.application.execution.ConcurrencyLimitExceededException;
import io.flowforge.application.execution.ConcurrencyPermitRecovery;
import io.flowforge.application.execution.TaskCompletion;
import io.flowforge.application.execution.TaskCompletionResult;
import io.flowforge.application.execution.TaskResult;
import io.flowforge.application.execution.TaskWorkItem;
import io.flowforge.application.execution.WorkflowNotPublishedException;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.domain.execution.ExecutionEventType;
import io.flowforge.domain.execution.TaskAttemptStatus;
import io.flowforge.domain.execution.TaskRunStatus;
import io.flowforge.domain.execution.WorkflowExecution;
import io.flowforge.domain.execution.WorkflowRunStatus;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.SecretReference;
import io.flowforge.domain.workflow.TaskDependency;
import io.flowforge.domain.workflow.TaskReliabilityPolicy;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.dao.DataIntegrityViolationException;
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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false",
        "flowforge.backpressure.max-ready-tasks-per-workflow=2"
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
    DurableTaskQueue durableTaskQueue;

    @Autowired
    ConcurrencyPermitRecovery concurrencyPermitRecovery;

    @Autowired
    JdbcClient jdbc;

    @BeforeEach
    void removeExecutions() {
        jdbc.sql("DELETE FROM workflow_execution").update();
        jdbc.sql("DELETE FROM coordination_permit WHERE resource_key LIKE 'concurrency:%'").update();
        jdbc.sql("DELETE FROM coordination_resource WHERE resource_key LIKE 'concurrency:%'").update();
        jdbc.sql("DELETE FROM coordination_resource WHERE resource_key LIKE 'backpressure:%'").update();
    }

    @Test
    void isolatesExecutionAdmissionIdempotencyReadsAndCancellationByTenant() {
        TenantId tenantA = registerTenant("merchant-a", "Merchant A");
        TenantId tenantB = registerTenant("merchant-b", "Merchant B");
        WorkflowDefinition workflowA = publish(tenantA, workflow(List.of(task("ROOT")), List.of()));
        WorkflowDefinition workflowB = publish(tenantB, workflow(List.of(task("ROOT")), List.of()));

        WorkflowExecution executionA = executionRepository.start(
                tenantA, workflowA.id(), "same-request", TIME
        );
        WorkflowExecution replayA = executionRepository.start(
                tenantA, workflowA.id(), "same-request", TIME.plusSeconds(1)
        );
        WorkflowExecution executionB = executionRepository.start(
                tenantB, workflowB.id(), "same-request", TIME
        );

        assertThat(replayA.workflow().id()).isEqualTo(executionA.workflow().id());
        assertThat(executionA.tenantId()).isEqualTo(tenantA);
        assertThat(executionB.tenantId()).isEqualTo(tenantB);
        assertThat(executionRepository.findById(tenantB, executionA.workflow().id())).isEmpty();
        assertThatThrownBy(() -> executionRepository.cancel(
                tenantB, executionA.workflow().id(), TIME.plusSeconds(2)
        )).isInstanceOf(ExecutionNotFoundException.class);
        assertThatThrownBy(() -> executionRepository.start(
                tenantB, workflowA.id(), "foreign-workflow", TIME.plusSeconds(2)
        )).isInstanceOf(WorkflowNotPublishedException.class);
        assertThat(executionRepository.findById(tenantA, executionA.workflow().id()))
                .get()
                .extracting(WorkflowExecution::tenantId)
                .isEqualTo(tenantA);
        assertThat(jdbc.sql("""
                SELECT COUNT(*) FROM execution_event
                 WHERE tenant_id = :tenantId
                   AND workflow_execution_id = :executionId
                """)
                .param("tenantId", tenantA.value())
                .param("executionId", executionA.workflow().id())
                .query(Long.class)
                .single()).isPositive();
        assertThat(jdbc.sql("""
                SELECT COUNT(*) FROM control_plane_outbox
                 WHERE tenant_id = :tenantId
                   AND workflow_execution_id = :executionId
                   AND payload ->> 'tenantId' = :tenantId
                """)
                .param("tenantId", tenantA.value())
                .param("executionId", executionA.workflow().id())
                .query(Long.class)
                .single()).isPositive();

        UUID workflowVersionId = jdbc.sql("""
                SELECT id FROM workflow_version
                 WHERE workflow_id = :workflowId AND version_status = 'PUBLISHED'
                """)
                .param("workflowId", workflowA.id())
                .query(UUID.class)
                .single();
        assertThatThrownBy(() -> jdbc.sql("""
                INSERT INTO workflow_execution(
                    id, tenant_id, workflow_id, workflow_version_id, workflow_version_number,
                    idempotency_key, status, state_version, created_at, started_at
                ) VALUES (
                    :id, :tenantId, :workflowId, :versionId, 1,
                    'ownership-mismatch', 'RUNNING', 0, :now, :now
                )
                """)
                .param("id", UUID.randomUUID())
                .param("tenantId", tenantB.value())
                .param("workflowId", workflowA.id())
                .param("versionId", workflowVersionId)
                .param("now", Timestamp.from(TIME))
                .update())
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void carriesSecretReferencesIntoCommandsWithoutInlineMaterial() {
        TaskDefinition task = new TaskDefinition(
                "PAY", "Pay", "PAYMENT", Map.of("amount", 42),
                Map.of("apiKey", new SecretReference("vault", "tenants/local/payment", "11")),
                null, null
        );
        WorkflowDefinition published = publish(workflow(List.of(task), List.of()));
        executionRepository.start(TenantId.LOCAL, published.id(), "secret-reference", TIME);

        assertThat(durableTaskQueue.enqueueReadyTasks(TenantId.LOCAL, 1, TIME.plusSeconds(1))).isEqualTo(1);

        String payload = jdbc.sql("""
                SELECT payload::text FROM control_plane_outbox
                 WHERE message_kind = 'TASK_COMMAND'
                 ORDER BY created_at DESC LIMIT 1
                """).query(String.class).single();
        assertThat(payload)
                .contains("secretReferences", "tenants/local/payment", "vault")
                .doesNotContain("resolvedSecret", "secretValue");
    }

    @Test
    void rejectsAdmissionBeforeThePerWorkflowReadyQueueCanGrowUnbounded() {
        WorkflowDefinition published = publish(workflow(List.of(task("ROOT")), List.of()));
        executionRepository.start(TenantId.LOCAL, published.id(), "queue-limit-1", TIME);
        executionRepository.start(TenantId.LOCAL, published.id(), "queue-limit-2", TIME);

        assertThatThrownBy(() -> executionRepository.start(
                TenantId.LOCAL, published.id(), "queue-limit-3", TIME.plusSeconds(1)
        )).isInstanceOf(AdmissionOverloadedException.class)
                .satisfies(failure -> {
                    AdmissionOverloadedException overloaded = (AdmissionOverloadedException) failure;
                    assertThat(overloaded.limit()).isEqualTo(2);
                    assertThat(overloaded.retryAfter()).isEqualTo(Duration.ofSeconds(1));
                });

        long ready = jdbc.sql("""
                SELECT COUNT(*) FROM task_execution te
                JOIN workflow_execution we ON we.id = te.workflow_execution_id
                WHERE we.workflow_id = :workflowId AND te.status = 'READY'
                """)
                .param("workflowId", published.id())
                .query(Long.class)
                .single();
        assertThat(ready).isEqualTo(2);
    }

    @Test
    void enforcesAndReleasesVersionedWorkflowConcurrency() {
        WorkflowDefinition published = publish(new WorkflowDraft(
                "Limited workflow",
                null,
                List.of(task("ROOT")),
                List.of(),
                1
        ));
        WorkflowExecution first = executionRepository.start(TenantId.LOCAL, published.id(), "limited-1", TIME);

        assertThatThrownBy(() -> executionRepository.start(
                TenantId.LOCAL, published.id(), "limited-2", TIME.plusSeconds(31)
        )).isInstanceOf(ConcurrencyLimitExceededException.class);

        TaskWorkItem task = onlyClaim(TIME.plusSeconds(32));
        executionRepository.completeTask(
                TaskCompletion.from(task, TaskResult.succeeded()),
                TIME.plusSeconds(33)
        );
        WorkflowExecution second = executionRepository.start(
                TenantId.LOCAL, published.id(), "limited-2", TIME.plusSeconds(34));

        assertThat(second.workflow().id()).isNotEqualTo(first.workflow().id());
        assertThat(activeConcurrencyPermits("concurrency:workflow-version:%")).isEqualTo(1);
    }

    @Test
    void taskConcurrencyLeavesSaturatedWorkReadyUntilCapacityIsReleased() {
        TaskDefinition limitedTask = new TaskDefinition(
                "ROOT", "ROOT", "NOOP", Map.of(), TaskReliabilityPolicy.defaults(), 1
        );
        WorkflowDefinition published = publish(new WorkflowDraft(
                "Task limited workflow", null, List.of(limitedTask), List.of()
        ));
        WorkflowExecution first = executionRepository.start(TenantId.LOCAL, published.id(), "task-limit-1", TIME);
        WorkflowExecution second = executionRepository.start(TenantId.LOCAL, published.id(), "task-limit-2", TIME);

        List<TaskWorkItem> firstBatch = executionRepository.claimReadyTasks(
                TenantId.LOCAL, 10, TIME.plusSeconds(1));
        assertThat(firstBatch).hasSize(1);
        WorkflowExecution waiting = executionRepository.findById(
                TenantId.LOCAL,
                firstBatch.getFirst().workflowRunId().equals(first.workflow().id())
                        ? second.workflow().id()
                        : first.workflow().id()
        ).orElseThrow();
        assertThat(waiting.tasks()).singleElement()
                .extracting(task -> task.status())
                .isEqualTo(TaskRunStatus.READY);
        assertThat(activeConcurrencyPermits("concurrency:task:%")).isEqualTo(1);

        executionRepository.completeTask(
                TaskCompletion.from(firstBatch.getFirst(), TaskResult.succeeded()),
                TIME.plusSeconds(2)
        );
        List<TaskWorkItem> secondBatch = executionRepository.claimReadyTasks(
                TenantId.LOCAL, 10, TIME.plusSeconds(3));

        assertThat(secondBatch).hasSize(1);
        assertThat(secondBatch.getFirst().workflowRunId()).isEqualTo(waiting.workflow().id());
        assertThat(activeConcurrencyPermits("concurrency:task:%")).isEqualTo(1);
    }

    @Test
    void retrySchedulingReleasesTaskCapacityForOtherExecutions() {
        TaskReliabilityPolicy retryPolicy = new TaskReliabilityPolicy(
                2, Duration.ofSeconds(10), 2.0, Duration.ofSeconds(10), 0.0, null, Set.of()
        );
        TaskDefinition limitedTask = new TaskDefinition(
                "ROOT", "ROOT", "NOOP", Map.of(), retryPolicy, 1
        );
        WorkflowDefinition published = publish(new WorkflowDraft(
                "Retry capacity", null, List.of(limitedTask), List.of()
        ));
        executionRepository.start(TenantId.LOCAL, published.id(), "retry-capacity-1", TIME);
        executionRepository.start(TenantId.LOCAL, published.id(), "retry-capacity-2", TIME);
        TaskWorkItem first = onlyClaim(TIME.plusSeconds(1));

        executionRepository.completeTask(
                TaskCompletion.from(first, TaskResult.retryableFailure("TRANSIENT", "try again")),
                TIME.plusSeconds(2)
        );
        TaskWorkItem replacement = onlyClaim(TIME.plusSeconds(3));

        assertThat(replacement.workflowRunId()).isNotEqualTo(first.workflowRunId());
        assertThat(activeConcurrencyPermits("concurrency:task:%")).isEqualTo(1);
    }

    @Test
    void rebuildsExpiredPermitsFromActivePostgresExecutionState() {
        WorkflowDefinition published = publish(new WorkflowDraft(
                "Recoverable limit", null, List.of(task("ROOT")), List.of(), 1
        ));
        WorkflowExecution execution = executionRepository.start(
                TenantId.LOCAL, published.id(), "recover-limit", TIME);
        UUID oldToken = workflowPermitToken(execution.workflow().id());

        int reconciled = concurrencyPermitRecovery.reconcileConcurrencyPermits(100, TIME.plusSeconds(31));
        UUID replacement = workflowPermitToken(execution.workflow().id());

        assertThat(reconciled).isPositive();
        assertThat(replacement).isNotEqualTo(oldToken);
        assertThat(activeConcurrencyPermits("concurrency:workflow-version:%")).isEqualTo(1);
        assertThatThrownBy(() -> executionRepository.start(
                TenantId.LOCAL, published.id(), "recover-limit-2", TIME.plusSeconds(32)
        )).isInstanceOf(ConcurrencyLimitExceededException.class);
    }

    @Test
    void serializesWorkflowAdmissionAcrossCompetingReplicas() throws Exception {
        WorkflowDefinition published = publish(new WorkflowDraft(
                "Replica-safe limit", null, List.of(task("ROOT")), List.of(), 2
        ));
        int callers = 8;
        CountDownLatch start = new CountDownLatch(1);
        List<Boolean> admitted;
        try (var pool = Executors.newFixedThreadPool(callers)) {
            var futures = java.util.stream.IntStream.range(0, callers)
                    .mapToObj(index -> pool.submit(() -> {
                        start.await();
                        try {
                            executionRepository.start(
                                    TenantId.LOCAL, published.id(), "replica-" + index, TIME);
                            return true;
                        } catch (ConcurrencyLimitExceededException | AdmissionOverloadedException saturated) {
                            return false;
                        }
                    }))
                    .toList();
            start.countDown();
            admitted = futures.stream().map(future -> {
                try {
                    return future.get(10, TimeUnit.SECONDS);
                } catch (Exception failure) {
                    throw new AssertionError(failure);
                }
            }).toList();
        }

        assertThat(admitted).filteredOn(Boolean::booleanValue).hasSize(2);
        assertThat(activeConcurrencyPermits("concurrency:workflow-version:%")).isEqualTo(2);
    }

    @Test
    void materializesTheLatestPublishedVersionIdempotently() {
        WorkflowDefinition published = publish(workflow(
                List.of(task("ROOT"), task("CHILD")),
                List.of(new TaskDependency("CHILD", "ROOT"))
        ));
        workflowService.update(
                TenantId.LOCAL,
                published.id(),
                published.lockVersion(),
                workflow(List.of(task("NEW_DRAFT_TASK")), List.of())
        );

        WorkflowExecution first = executionRepository.start(TenantId.LOCAL, published.id(), "request-1", TIME);
        WorkflowExecution duplicate = executionRepository.start(
                TenantId.LOCAL, published.id(), "request-1", TIME.plusSeconds(1));

        assertThat(duplicate.workflow().id()).isEqualTo(first.workflow().id());
        assertThat(first.workflow().workflowVersion()).isEqualTo(1);
        assertThat(first.tasks()).extracting(task -> task.taskKey() + ":" + task.status())
                .containsExactly("ROOT:READY", "CHILD:BLOCKED");
    }

    @Test
    void rejectsExecutionOfAnUnpublishedWorkflow() {
        WorkflowDefinition draft = workflowService.create(
                TenantId.LOCAL, workflow(List.of(task("ROOT")), List.of()));

        assertThatThrownBy(() -> executionRepository.start(
                TenantId.LOCAL, draft.id(), "request-1", TIME))
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
        executionRepository.start(TenantId.LOCAL, published.id(), "attempt-timeout", TIME);

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
        WorkflowExecution execution = executionRepository.start(TenantId.LOCAL, published.id(), "fan-in", TIME);

        TaskWorkItem root = onlyClaim(TIME.plusSeconds(1));
        executionRepository.completeTask(TaskCompletion.from(root, TaskResult.succeeded()), TIME.plusSeconds(2));

        List<TaskWorkItem> branches = executionRepository.claimReadyTasks(
                TenantId.LOCAL, 10, TIME.plusSeconds(3));
        assertThat(branches).extracting(TaskWorkItem::taskKey)
                .containsExactlyInAnyOrder("LEFT", "RIGHT");
        TaskWorkItem left = byKey(branches, "LEFT");
        TaskWorkItem right = byKey(branches, "RIGHT");

        executionRepository.completeTask(TaskCompletion.from(left, TaskResult.succeeded()), TIME.plusSeconds(4));
        assertThat(executionRepository.claimReadyTasks(
                TenantId.LOCAL, 10, TIME.plusSeconds(5))).isEmpty();

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
        executionRepository.start(TenantId.LOCAL, published.id(), "failure", TIME);
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
        WorkflowExecution started = executionRepository.start(TenantId.LOCAL, published.id(), "cancel", TIME);
        TaskWorkItem root = onlyClaim(TIME.plusSeconds(1));

        WorkflowExecution cancelling = executionRepository.cancel(
                TenantId.LOCAL, started.workflow().id(), TIME.plusSeconds(2));
        assertThat(cancelling.workflow().status()).isEqualTo(WorkflowRunStatus.CANCELLING);
        assertThat(cancelling.tasks()).filteredOn(task -> task.taskKey().equals("CHILD"))
                .extracting(task -> task.status())
                .containsExactly(TaskRunStatus.CANCELLED);

        WorkflowExecution cancelled = executionRepository.completeTask(
                TaskCompletion.from(root, TaskResult.succeeded()),
                TIME.plusSeconds(3)
        ).execution();
        WorkflowExecution duplicateCancel = executionRepository.cancel(
                TenantId.LOCAL, started.workflow().id(),
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
        WorkflowExecution execution = executionRepository.start(
                TenantId.LOCAL, published.id(), "concurrency", TIME);
        TaskWorkItem root = onlyClaim(TIME.plusSeconds(1));
        executionRepository.completeTask(TaskCompletion.from(root, TaskResult.succeeded()), TIME.plusSeconds(2));
        List<TaskWorkItem> branches = executionRepository.claimReadyTasks(
                TenantId.LOCAL, 10, TIME.plusSeconds(3));
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
        WorkflowExecution current = executionRepository.findById(
                TenantId.LOCAL, execution.workflow().id()).orElseThrow();
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
        List<TaskWorkItem> claimed = executionRepository.claimReadyTasks(TenantId.LOCAL, 10, time);
        assertThat(claimed).hasSize(1);
        return claimed.getFirst();
    }

    private long activeConcurrencyPermits(String resourcePattern) {
        return jdbc.sql("""
                SELECT COUNT(*) FROM coordination_permit
                 WHERE resource_key LIKE :resourcePattern
                   AND status = 'ACTIVE'
                """)
                .param("resourcePattern", resourcePattern)
                .query(Long.class)
                .single();
    }

    private UUID workflowPermitToken(UUID executionId) {
        return jdbc.sql("""
                SELECT concurrency_permit_token FROM workflow_execution WHERE id = :executionId
                """)
                .param("executionId", executionId)
                .query(UUID.class)
                .single();
    }

    private static TaskWorkItem byKey(List<TaskWorkItem> items, String key) {
        return items.stream().filter(item -> item.taskKey().equals(key)).findFirst().orElseThrow();
    }

    private WorkflowDefinition publish(WorkflowDraft draft) {
        WorkflowDefinition created = workflowService.create(TenantId.LOCAL, draft);
        return workflowService.publish(TenantId.LOCAL, created.id(), created.lockVersion());
    }

    private WorkflowDefinition publish(TenantId tenantId, WorkflowDraft draft) {
        WorkflowDefinition created = workflowService.create(tenantId, draft);
        return workflowService.publish(tenantId, created.id(), created.lockVersion());
    }

    private TenantId registerTenant(String tenantId, String displayName) {
        jdbc.sql("""
                INSERT INTO tenant_registry(tenant_id, display_name, status)
                VALUES (:tenantId, :displayName, 'ACTIVE')
                ON CONFLICT (tenant_id) DO NOTHING
                """)
                .param("tenantId", tenantId)
                .param("displayName", displayName)
                .update();
        return new TenantId(tenantId);
    }

    private static WorkflowDraft workflow(List<TaskDefinition> tasks, List<TaskDependency> dependencies) {
        return new WorkflowDraft("Execution test", null, tasks, dependencies);
    }

    private static TaskDefinition task(String key) {
        return new TaskDefinition(key, key, "NOOP", Map.of());
    }
}
