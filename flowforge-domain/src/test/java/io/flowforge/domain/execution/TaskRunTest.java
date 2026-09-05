package io.flowforge.domain.execution;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskRunTest {
    private static final UUID TASK_RUN_ID = UUID.fromString("30000000-0000-0000-0000-000000000001");
    private static final UUID WORKFLOW_RUN_ID = UUID.fromString("10000000-0000-0000-0000-000000000001");
    private static final Instant CREATED_AT = Instant.parse("2026-09-03T08:00:00Z");

    @Test
    void progressesFromBlockedToSuccessfulCompletion() {
        Instant startedAt = CREATED_AT.plusSeconds(2);
        Instant finishedAt = CREATED_AT.plusSeconds(5);

        TaskRun blocked = TaskRun.blocked(TASK_RUN_ID, WORKFLOW_RUN_ID, "PROCESS_PAYMENT", CREATED_AT);
        TaskRun ready = blocked.transitionTo(TaskRunStatus.READY, CREATED_AT.plusSeconds(1));
        TaskRun running = ready.transitionTo(TaskRunStatus.RUNNING, startedAt);
        TaskRun succeeded = running.transitionTo(TaskRunStatus.SUCCEEDED, finishedAt);

        assertThat(blocked.status()).isEqualTo(TaskRunStatus.BLOCKED);
        assertThat(ready.status()).isEqualTo(TaskRunStatus.READY);
        assertThat(running.startedAt()).isEqualTo(startedAt);
        assertThat(succeeded.finishedAt()).isEqualTo(finishedAt);
        assertThat(succeeded.stateVersion()).isEqualTo(3);
        assertThat(succeeded.status().isTerminal()).isTrue();
    }

    @Test
    void createsRootTasksAsReady() {
        TaskRun ready = TaskRun.ready(TASK_RUN_ID, WORKFLOW_RUN_ID, "VALIDATE_ORDER", CREATED_AT);

        assertThat(ready.status()).isEqualTo(TaskRunStatus.READY);
        assertThat(ready.stateVersion()).isZero();
        assertThat(ready.startedAt()).isNull();
        assertThat(ready.finishedAt()).isNull();
    }

    @ParameterizedTest
    @EnumSource(value = TaskRunStatus.class, names = {"FAILED", "TIMED_OUT", "CANCELLED"})
    void recordsEveryRunningTerminalOutcome(TaskRunStatus terminalStatus) {
        Instant startedAt = CREATED_AT.plusSeconds(1);
        Instant finishedAt = CREATED_AT.plusSeconds(2);
        TaskRun running = TaskRun.ready(TASK_RUN_ID, WORKFLOW_RUN_ID, "TASK", CREATED_AT)
                .transitionTo(TaskRunStatus.RUNNING, startedAt);

        TaskRun terminal = running.transitionTo(terminalStatus, finishedAt);

        assertThat(terminal.status()).isEqualTo(terminalStatus);
        assertThat(terminal.startedAt()).isEqualTo(startedAt);
        assertThat(terminal.finishedAt()).isEqualTo(finishedAt);
        assertThat(terminal.status().isTerminal()).isTrue();
    }

    @Test
    void cancelsWorkThatHasNotStarted() {
        Instant cancelledAt = CREATED_AT.plusSeconds(1);

        TaskRun cancelled = TaskRun.blocked(TASK_RUN_ID, WORKFLOW_RUN_ID, "TASK", CREATED_AT)
                .transitionTo(TaskRunStatus.CANCELLED, cancelledAt);

        assertThat(cancelled.startedAt()).isNull();
        assertThat(cancelled.finishedAt()).isEqualTo(cancelledAt);
    }

    @Test
    void rejectsSkippingReadinessAndDuplicateCompletion() {
        TaskRun blocked = TaskRun.blocked(TASK_RUN_ID, WORKFLOW_RUN_ID, "TASK", CREATED_AT);

        assertThatThrownBy(() -> blocked.transitionTo(TaskRunStatus.RUNNING, CREATED_AT.plusSeconds(1)))
                .isInstanceOf(InvalidStateTransitionException.class)
                .hasMessage("Invalid task run transition from BLOCKED to RUNNING");

        TaskRun succeeded = TaskRun.ready(TASK_RUN_ID, WORKFLOW_RUN_ID, "TASK", CREATED_AT)
                .transitionTo(TaskRunStatus.RUNNING, CREATED_AT.plusSeconds(1))
                .transitionTo(TaskRunStatus.SUCCEEDED, CREATED_AT.plusSeconds(2));

        assertThatThrownBy(() -> succeeded.transitionTo(TaskRunStatus.SUCCEEDED, CREATED_AT.plusSeconds(3)))
                .isInstanceOf(InvalidStateTransitionException.class);
    }

    @Test
    void rejectsCompletionBeforeStartTime() {
        TaskRun running = TaskRun.ready(TASK_RUN_ID, WORKFLOW_RUN_ID, "TASK", CREATED_AT)
                .transitionTo(TaskRunStatus.RUNNING, CREATED_AT.plusSeconds(2));

        assertThatThrownBy(() -> running.transitionTo(TaskRunStatus.SUCCEEDED, CREATED_AT.plusSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("transition time must not precede start time");
    }

    @Test
    void schedulesAndReleasesADurableRetryWithoutLosingTheFirstStartTime() {
        Instant startedAt = CREATED_AT.plusSeconds(1);
        Instant failedAt = CREATED_AT.plusSeconds(2);
        Instant retryAt = CREATED_AT.plusSeconds(12);
        TaskRun running = TaskRun.ready(TASK_RUN_ID, WORKFLOW_RUN_ID, "TASK", CREATED_AT)
                .transitionTo(TaskRunStatus.RUNNING, startedAt);

        TaskRun scheduled = running.scheduleRetry(failedAt, retryAt);
        TaskRun readyAgain = scheduled.transitionTo(TaskRunStatus.READY, retryAt);
        TaskRun runningAgain = readyAgain.transitionTo(TaskRunStatus.RUNNING, retryAt);

        assertThat(scheduled.status()).isEqualTo(TaskRunStatus.RETRY_SCHEDULED);
        assertThat(scheduled.nextAttemptAt()).isEqualTo(retryAt);
        assertThat(readyAgain.nextAttemptAt()).isNull();
        assertThat(runningAgain.startedAt()).isEqualTo(startedAt);
        assertThat(runningAgain.stateVersion()).isEqualTo(4);
    }

    @Test
    void rejectsRetrySchedulingWithoutARunningAttemptOrWithAPastDueTime() {
        TaskRun ready = TaskRun.ready(TASK_RUN_ID, WORKFLOW_RUN_ID, "TASK", CREATED_AT);

        assertThatThrownBy(() -> ready.scheduleRetry(CREATED_AT.plusSeconds(1), CREATED_AT.plusSeconds(2)))
                .isInstanceOf(InvalidStateTransitionException.class);

        TaskRun running = ready.transitionTo(TaskRunStatus.RUNNING, CREATED_AT.plusSeconds(1));
        assertThatThrownBy(() -> running.scheduleRetry(
                CREATED_AT.plusSeconds(3), CREATED_AT.plusSeconds(2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("retryAt must not precede the scheduling time");
    }
}
