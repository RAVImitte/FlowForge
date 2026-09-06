package io.flowforge.application.execution;

import io.flowforge.application.coordination.TokenBucketDecision;
import io.flowforge.application.coordination.TokenBucketPolicy;
import io.flowforge.application.coordination.TokenBucketRateLimiter;
import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.domain.execution.WorkflowExecution;
import io.flowforge.domain.execution.WorkflowRun;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class WorkflowExecutionServiceTest {
    private static final Instant NOW = Instant.parse("2026-09-03T12:00:00Z");
    private static final UUID WORKFLOW_ID = UUID.fromString("10000000-0000-0000-0000-000000000001");
    private static final UUID EXECUTION_ID = UUID.fromString("20000000-0000-0000-0000-000000000001");

    private ExecutionRepository repository;
    private TaskDispatcher dispatcher;
    private WorkflowExecutionService service;

    @BeforeEach
    void setUp() {
        repository = mock(ExecutionRepository.class);
        dispatcher = mock(TaskDispatcher.class);
        service = new WorkflowExecutionService(
                repository,
                dispatcher,
                Clock.fixed(NOW, ZoneOffset.UTC),
                (workItem, failure) -> {
                    throw new AssertionError("Unexpected completion failure", failure);
                }
        );
        when(repository.readyQueue(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyInt()
        )).thenAnswer(invocation -> ReadyQueueSnapshot.unknown(invocation.getArgument(2)));
        when(repository.readyTenants(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyInt()))
                .thenReturn(List.of(TenantId.LOCAL));
    }

    @Test
    void startsWithANormalizedIdempotencyKey() {
        WorkflowExecution execution = execution();
        when(repository.start(TenantId.LOCAL, WORKFLOW_ID, "request-1", NOW)).thenReturn(execution);
        when(repository.claimReadyTasks(TenantId.LOCAL, 100, NOW)).thenReturn(List.of());
        when(repository.findById(TenantId.LOCAL, EXECUTION_ID)).thenReturn(Optional.of(execution));

        WorkflowExecution result = service.start(TenantId.LOCAL, WORKFLOW_ID, "  request-1  ");

        assertThat(result).isSameAs(execution);
        verify(repository).start(TenantId.LOCAL, WORKFLOW_ID, "request-1", NOW);
    }

    @Test
    void rejectsMissingIdempotencyKeys() {
        assertThatThrownBy(() -> service.start(TenantId.LOCAL, WORKFLOW_ID, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Idempotency-Key must not be blank");
    }

    @Test
    void leavesInitialTasksForTheDurableDispatcherWhenEagerDispatchIsDisabled() {
        WorkflowExecution execution = execution();
        WorkflowExecutionService distributed = new WorkflowExecutionService(
                repository,
                dispatcher,
                Clock.fixed(NOW, ZoneOffset.UTC),
                (workItem, failure) -> { },
                (tenantId, key, policy, requested, now) -> new TokenBucketDecision(requested, Duration.ZERO),
                new TokenBucketPolicy(100, 100, Duration.ofSeconds(1)),
                new AdmissionBackpressureObserver() { },
                false
        );
        when(repository.start(TenantId.LOCAL, WORKFLOW_ID, "request-1", NOW)).thenReturn(execution);
        when(repository.findById(TenantId.LOCAL, EXECUTION_ID)).thenReturn(Optional.of(execution));

        assertThat(distributed.start(TenantId.LOCAL, WORKFLOW_ID, "request-1")).isSameAs(execution);

        verify(repository, never()).readyQueue(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyInt()
        );
        verifyNoInteractions(dispatcher);
    }

    @Test
    void persistsAsynchronousTaskResults() {
        UUID taskId = UUID.fromString("30000000-0000-0000-0000-000000000001");
        TaskWorkItem workItem = new TaskWorkItem(
                EXECUTION_ID, taskId, "VALIDATE", "NOOP", Map.of(), 1, 1
        );
        when(repository.claimReadyTasks(TenantId.LOCAL, 10, NOW)).thenReturn(List.of(workItem));
        when(dispatcher.dispatch(workItem)).thenReturn(CompletableFuture.completedFuture(TaskResult.succeeded()));
        when(repository.completeTask(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq(NOW)))
                .thenReturn(new TaskCompletionResult(execution(), true));

        assertThat(service.dispatchReadyTasks(10)).isEqualTo(1);

        ArgumentCaptor<TaskCompletion> completion = ArgumentCaptor.forClass(TaskCompletion.class);
        verify(repository).completeTask(completion.capture(), org.mockito.ArgumentMatchers.eq(NOW));
        assertThat(completion.getValue().taskRunId()).isEqualTo(taskId);
        assertThat(completion.getValue().expectedStateVersion()).isEqualTo(1);
        assertThat(completion.getValue().outcome()).isEqualTo(TaskOutcome.SUCCEEDED);
    }

    @Test
    void rateLimitsTaskClaimsBeforeChangingDurableTaskState() {
        TokenBucketRateLimiter limiter = mock(TokenBucketRateLimiter.class);
        AdmissionBackpressureObserver observer = mock(AdmissionBackpressureObserver.class);
        TokenBucketPolicy policy = new TokenBucketPolicy(2, 1, Duration.ofSeconds(1));
        WorkflowExecutionService limited = new WorkflowExecutionService(
                repository, dispatcher, Clock.fixed(NOW, ZoneOffset.UTC),
                (workItem, failure) -> { }, limiter, policy, observer
        );
        when(repository.readyTenants(NOW, 10)).thenReturn(List.of(TenantId.LOCAL));
        when(repository.readyQueue(TenantId.LOCAL, NOW, 10))
                .thenReturn(new ReadyQueueSnapshot(4, Duration.ofSeconds(9)));
        when(limiter.consume(TenantId.LOCAL, "task-dispatch", policy, 4, NOW))
                .thenReturn(new TokenBucketDecision(1, Duration.ofSeconds(1)));
        when(repository.claimReadyTasks(TenantId.LOCAL, 1, NOW)).thenReturn(List.of());

        assertThat(limited.dispatchReadyTasks(10)).isZero();

        verify(observer).readyQueueObserved(4, Duration.ofSeconds(9));
        verify(observer).taskDispatchThrottled(4, 1, Duration.ofSeconds(1));
        verify(repository).claimReadyTasks(TenantId.LOCAL, 1, NOW);
    }

    private static WorkflowExecution execution() {
        WorkflowRun workflow = WorkflowRun.pending(EXECUTION_ID, WORKFLOW_ID, 1, NOW)
                .transitionTo(io.flowforge.domain.execution.WorkflowRunStatus.RUNNING, NOW);
        return new WorkflowExecution(TenantId.LOCAL, workflow, List.of(), List.of(), List.of());
    }
}
