package io.flowforge.application.schedule;

import io.flowforge.application.execution.WorkflowNotPublishedException;
import io.flowforge.application.workflow.PageResult;
import io.flowforge.application.workflow.WorkflowNotFoundException;
import io.flowforge.application.workflow.WorkflowRepository;
import io.flowforge.domain.schedule.ScheduleStatus;
import io.flowforge.domain.schedule.WorkflowSchedule;
import io.flowforge.domain.schedule.WorkflowScheduleDraft;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public final class WorkflowScheduleService {
    private final ScheduleRepository schedules;
    private final WorkflowRepository workflows;
    private final ScheduleCalculator calculator;
    private final Clock clock;

    public WorkflowScheduleService(
            ScheduleRepository schedules,
            WorkflowRepository workflows,
            ScheduleCalculator calculator,
            Clock clock
    ) {
        this.schedules = Objects.requireNonNull(schedules);
        this.workflows = Objects.requireNonNull(workflows);
        this.calculator = Objects.requireNonNull(calculator);
        this.clock = Objects.requireNonNull(clock);
    }

    public WorkflowSchedule create(WorkflowScheduleDraft draft) {
        validateWorkflow(draft.workflowId());
        Instant now = clock.instant();
        return schedules.create(draft, calculator.nextFireAt(draft.spec(), now), now);
    }

    public WorkflowSchedule get(UUID id) {
        return schedules.findById(id).orElseThrow(() -> new ScheduleNotFoundException(id));
    }

    public PageResult<WorkflowSchedule> list(int page, int size) {
        if (page < 0) throw new IllegalArgumentException("page must be at least 0");
        if (size < 1 || size > 100) throw new IllegalArgumentException("size must be between 1 and 100");
        return schedules.findAll(page, size);
    }

    public WorkflowSchedule update(UUID id, long expectedLockVersion, WorkflowScheduleDraft draft) {
        validateWorkflow(draft.workflowId());
        Instant now = clock.instant();
        return schedules.update(
                id,
                expectedLockVersion,
                draft,
                calculator.nextFireAt(draft.spec(), now),
                now
        );
    }

    public WorkflowSchedule pause(UUID id, long expectedLockVersion) {
        WorkflowSchedule current = get(id);
        return schedules.changeStatus(
                id, expectedLockVersion, ScheduleStatus.PAUSED, current.nextFireAt(), clock.instant()
        );
    }

    public WorkflowSchedule resume(UUID id, long expectedLockVersion) {
        WorkflowSchedule current = get(id);
        Instant now = clock.instant();
        return schedules.changeStatus(
                id,
                expectedLockVersion,
                ScheduleStatus.ACTIVE,
                calculator.nextFireAt(current.spec(), now),
                now
        );
    }

    public void delete(UUID id, long expectedLockVersion) {
        schedules.delete(id, expectedLockVersion, clock.instant());
    }

    private void validateWorkflow(UUID workflowId) {
        if (workflows.findById(workflowId).isEmpty()) throw new WorkflowNotFoundException(workflowId);
        if (!workflows.hasPublishedVersion(workflowId)) throw new WorkflowNotPublishedException(workflowId);
    }
}
