package io.flowforge.domain.execution;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowRunTest {
    private static final UUID RUN_ID = UUID.fromString("10000000-0000-0000-0000-000000000001");
    private static final UUID WORKFLOW_ID = UUID.fromString("20000000-0000-0000-0000-000000000001");
    private static final Instant CREATED_AT = Instant.parse("2026-09-03T08:00:00Z");

    @Test
    void progressesFromPendingToSuccessfulCompletion() {
        Instant startedAt = CREATED_AT.plusSeconds(1);
        Instant finishedAt = CREATED_AT.plusSeconds(5);

        WorkflowRun pending = WorkflowRun.pending(RUN_ID, WORKFLOW_ID, 3, CREATED_AT);
        WorkflowRun running = pending.transitionTo(WorkflowRunStatus.RUNNING, startedAt);
        WorkflowRun succeeded = running.transitionTo(WorkflowRunStatus.SUCCEEDED, finishedAt);

        assertThat(pending.status()).isEqualTo(WorkflowRunStatus.PENDING);
        assertThat(pending.stateVersion()).isZero();
        assertThat(running.status()).isEqualTo(WorkflowRunStatus.RUNNING);
        assertThat(running.startedAt()).isEqualTo(startedAt);
        assertThat(running.stateVersion()).isEqualTo(1);
        assertThat(succeeded.status()).isEqualTo(WorkflowRunStatus.SUCCEEDED);
        assertThat(succeeded.startedAt()).isEqualTo(startedAt);
        assertThat(succeeded.finishedAt()).isEqualTo(finishedAt);
        assertThat(succeeded.stateVersion()).isEqualTo(2);
        assertThat(succeeded.status().isTerminal()).isTrue();
    }

    @Test
    void cancelsBeforeAWorkflowStarts() {
        Instant cancelledAt = CREATED_AT.plusSeconds(1);

        WorkflowRun cancelled = WorkflowRun.pending(RUN_ID, WORKFLOW_ID, 1, CREATED_AT)
                .transitionTo(WorkflowRunStatus.CANCELLED, cancelledAt);

        assertThat(cancelled.status()).isEqualTo(WorkflowRunStatus.CANCELLED);
        assertThat(cancelled.startedAt()).isNull();
        assertThat(cancelled.finishedAt()).isEqualTo(cancelledAt);
    }

    @Test
    void requiresCancellationCoordinationForARunningWorkflow() {
        WorkflowRun running = WorkflowRun.pending(RUN_ID, WORKFLOW_ID, 1, CREATED_AT)
                .transitionTo(WorkflowRunStatus.RUNNING, CREATED_AT.plusSeconds(1));

        assertThatThrownBy(() -> running.transitionTo(WorkflowRunStatus.CANCELLED, CREATED_AT.plusSeconds(2)))
                .isInstanceOf(InvalidStateTransitionException.class)
                .hasMessage("Invalid workflow run transition from RUNNING to CANCELLED");

        WorkflowRun cancelling = running.transitionTo(
                WorkflowRunStatus.CANCELLING,
                CREATED_AT.plusSeconds(2)
        );
        WorkflowRun cancelled = cancelling.transitionTo(
                WorkflowRunStatus.CANCELLED,
                CREATED_AT.plusSeconds(3)
        );

        assertThat(cancelling.finishedAt()).isNull();
        assertThat(cancelled.finishedAt()).isEqualTo(CREATED_AT.plusSeconds(3));
        assertThat(cancelled.stateVersion()).isEqualTo(3);
    }

    @Test
    void rejectsTransitionsFromTerminalStates() {
        WorkflowRun failed = WorkflowRun.pending(RUN_ID, WORKFLOW_ID, 1, CREATED_AT)
                .transitionTo(WorkflowRunStatus.RUNNING, CREATED_AT.plusSeconds(1))
                .transitionTo(WorkflowRunStatus.FAILED, CREATED_AT.plusSeconds(2));

        assertThatThrownBy(() -> failed.transitionTo(WorkflowRunStatus.RUNNING, CREATED_AT.plusSeconds(3)))
                .isInstanceOf(InvalidStateTransitionException.class);
    }

    @Test
    void rejectsOutOfOrderTransitionTimes() {
        WorkflowRun pending = WorkflowRun.pending(RUN_ID, WORKFLOW_ID, 1, CREATED_AT);

        assertThatThrownBy(() -> pending.transitionTo(WorkflowRunStatus.RUNNING, CREATED_AT.minusMillis(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("transition time must not precede creation time");
    }
}
