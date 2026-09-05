package io.flowforge.application.schedule;

import io.flowforge.application.execution.WorkflowExecutionService;
import io.flowforge.application.execution.WorkflowNotPublishedException;
import io.flowforge.domain.execution.WorkflowExecution;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public final class ScheduleFireService {
    private final ScheduleFireRepository fires;
    private final WorkflowExecutionService executions;
    private final Clock clock;
    private final String instanceId;
    private final Duration leaseDuration;
    private final Duration retryDelay;
    private final Duration misfireThreshold;

    public ScheduleFireService(
            ScheduleFireRepository fires,
            WorkflowExecutionService executions,
            Clock clock,
            String instanceId,
            Duration leaseDuration,
            Duration retryDelay,
            Duration misfireThreshold
    ) {
        this.fires = Objects.requireNonNull(fires);
        this.executions = Objects.requireNonNull(executions);
        this.clock = Objects.requireNonNull(clock);
        if (instanceId == null || instanceId.isBlank()) {
            throw new IllegalArgumentException("instanceId must not be blank");
        }
        this.instanceId = instanceId.strip();
        this.leaseDuration = positive(leaseDuration, "leaseDuration");
        this.retryDelay = positive(retryDelay, "retryDelay");
        this.misfireThreshold = positive(misfireThreshold, "misfireThreshold");
    }

    public ScheduleFireRunResult runOnce(int materializationLimit, int processingLimit) {
        validateLimit(materializationLimit, "materializationLimit");
        validateLimit(processingLimit, "processingLimit");
        Instant now = clock.instant();
        ScheduleMaterializationResult materialized = fires.materializeDue(
                materializationLimit, now, misfireThreshold
        );
        List<ClaimedScheduleFire> claimed = fires.claimPending(
                processingLimit, instanceId, now, leaseDuration
        );

        int started = 0;
        int failed = 0;
        int released = 0;
        int stale = 0;
        for (ClaimedScheduleFire fire : claimed) {
            try {
                WorkflowExecution execution = executions.start(fire.workflowId(), fire.idempotencyKey());
                if (fires.markStarted(
                        fire.triggerId(), fire.claimToken(), execution.workflow().id(), clock.instant()
                )) {
                    started++;
                } else {
                    stale++;
                }
            } catch (WorkflowNotPublishedException permanentFailure) {
                if (fires.markFailed(
                        fire.triggerId(), fire.claimToken(), rootMessage(permanentFailure), clock.instant()
                )) {
                    failed++;
                } else {
                    stale++;
                }
            } catch (RuntimeException transientFailure) {
                if (fires.release(
                        fire.triggerId(),
                        fire.claimToken(),
                        rootMessage(transientFailure),
                        clock.instant().plus(retryDelay)
                )) {
                    released++;
                } else {
                    stale++;
                }
            }
        }
        return new ScheduleFireRunResult(
                materialized.due(),
                materialized.skipped(),
                claimed.size(),
                started,
                failed,
                released,
                stale
        );
    }

    public static String idempotencyKey(java.util.UUID scheduleId, Instant scheduledFireAt) {
        Objects.requireNonNull(scheduleId, "scheduleId must not be null");
        Objects.requireNonNull(scheduledFireAt, "scheduledFireAt must not be null");
        return "schedule/" + scheduleId + "/"
                + scheduledFireAt.getEpochSecond() + "/" + scheduledFireAt.getNano();
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static void validateLimit(int limit, String name) {
        if (limit < 1 || limit > 1_000) {
            throw new IllegalArgumentException(name + " must be between 1 and 1000");
        }
    }

    private static String rootMessage(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null) current = current.getCause();
        String message = current.getMessage();
        return message == null ? current.getClass().getSimpleName() : message;
    }
}
