package io.flowforge.application.schedule;

import io.flowforge.application.execution.WorkflowExecutionService;
import io.flowforge.application.execution.WorkflowNotPublishedException;
import io.flowforge.domain.execution.WorkflowExecution;
import io.flowforge.domain.execution.WorkflowRun;
import io.flowforge.domain.execution.WorkflowRunStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ScheduleFireServiceTest {
    private static final Instant NOW = Instant.parse("2026-09-05T12:00:00Z");
    private static final UUID WORKFLOW_ID = UUID.fromString("10000000-0000-0000-0000-000000000001");
    private static final UUID EXECUTION_ID = UUID.fromString("20000000-0000-0000-0000-000000000001");

    private ScheduleFireRepository repository;
    private WorkflowExecutionService executions;
    private ScheduleFireService service;

    @BeforeEach
    void setUp() {
        repository = mock(ScheduleFireRepository.class);
        executions = mock(WorkflowExecutionService.class);
        service = new ScheduleFireService(
                repository,
                executions,
                Clock.fixed(NOW, ZoneOffset.UTC),
                "scheduler-1",
                Duration.ofSeconds(30),
                Duration.ofSeconds(5),
                Duration.ofMinutes(1)
        );
        when(repository.materializeDue(10, NOW, Duration.ofMinutes(1)))
                .thenReturn(ScheduleMaterializationResult.empty());
    }

    @Test
    void startsClaimedWorkAndAcknowledgesItWithTheClaimToken() {
        ClaimedScheduleFire fire = fire(1);
        when(repository.claimPending(10, "scheduler-1", NOW, Duration.ofSeconds(30)))
                .thenReturn(List.of(fire));
        when(executions.start(WORKFLOW_ID, fire.idempotencyKey())).thenReturn(execution());
        when(repository.markStarted(fire.triggerId(), fire.claimToken(), EXECUTION_ID, NOW))
                .thenReturn(true);

        ScheduleFireRunResult result = service.runOnce(10, 10);

        assertThat(result.started()).isEqualTo(1);
        assertThat(result.released()).isZero();
        verify(repository).markStarted(fire.triggerId(), fire.claimToken(), EXECUTION_ID, NOW);
    }

    @Test
    void terminatesPermanentFailuresAndReleasesTransientFailuresWithBackoff() {
        ClaimedScheduleFire permanent = fire(1);
        ClaimedScheduleFire transientFire = fire(2);
        when(repository.claimPending(10, "scheduler-1", NOW, Duration.ofSeconds(30)))
                .thenReturn(List.of(permanent, transientFire));
        when(executions.start(eq(WORKFLOW_ID), any()))
                .thenThrow(new WorkflowNotPublishedException(WORKFLOW_ID))
                .thenThrow(new IllegalStateException("database unavailable"));
        when(repository.markFailed(
                permanent.triggerId(), permanent.claimToken(),
                "Workflow has no published version: " + WORKFLOW_ID, NOW
        )).thenReturn(true);
        when(repository.release(
                transientFire.triggerId(), transientFire.claimToken(),
                "database unavailable", NOW.plusSeconds(5)
        )).thenReturn(true);

        ScheduleFireRunResult result = service.runOnce(10, 10);

        assertThat(result.failed()).isEqualTo(1);
        assertThat(result.released()).isEqualTo(1);
        verify(repository).markFailed(
                permanent.triggerId(), permanent.claimToken(),
                "Workflow has no published version: " + WORKFLOW_ID, NOW
        );
        ArgumentCaptor<Instant> availableAt = ArgumentCaptor.forClass(Instant.class);
        verify(repository).release(
                eq(transientFire.triggerId()),
                eq(transientFire.claimToken()),
                eq("database unavailable"),
                availableAt.capture()
        );
        assertThat(availableAt.getValue()).isEqualTo(NOW.plusSeconds(5));
    }

    @Test
    void derivesAStableFireIdentityFromTheLogicalOccurrence() {
        UUID scheduleId = UUID.fromString("30000000-0000-0000-0000-000000000001");
        Instant fireAt = Instant.parse("2026-09-05T12:34:56.123456Z");

        String first = ScheduleFireService.idempotencyKey(scheduleId, fireAt);
        String duplicate = ScheduleFireService.idempotencyKey(scheduleId, fireAt);

        assertThat(first).isEqualTo(duplicate);
        assertThat(first).contains(scheduleId.toString(), "1788611696", "123456000");
        assertThat(first).hasSizeLessThanOrEqualTo(200);
    }

    private static ClaimedScheduleFire fire(int suffix) {
        return new ClaimedScheduleFire(
                UUID.randomUUID(),
                UUID.randomUUID(),
                WORKFLOW_ID,
                NOW.minusSeconds(suffix),
                "schedule/test/" + suffix,
                1,
                UUID.randomUUID()
        );
    }

    private static WorkflowExecution execution() {
        WorkflowRun workflow = WorkflowRun.pending(EXECUTION_ID, WORKFLOW_ID, 1, NOW)
                .transitionTo(WorkflowRunStatus.RUNNING, NOW);
        return new WorkflowExecution(workflow, List.of(), List.of(), List.of());
    }
}
