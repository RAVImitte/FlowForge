package io.flowforge.application.schedule;

import io.flowforge.application.execution.WorkflowNotPublishedException;
import io.flowforge.application.workflow.PageResult;
import io.flowforge.application.workflow.WorkflowNotFoundException;
import io.flowforge.application.workflow.WorkflowRepository;
import io.flowforge.domain.schedule.ScheduleStatus;
import io.flowforge.domain.schedule.WorkflowSchedule;
import io.flowforge.domain.schedule.WorkflowScheduleDraft;
import io.flowforge.domain.tenancy.TenantId;

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

    public WorkflowSchedule create(TenantId tenantId, WorkflowScheduleDraft draft) {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        validateWorkflow(tenantId, draft.workflowId());
        Instant now = clock.instant();
        return schedules.create(tenantId, draft, calculator.nextFireAt(draft.spec(), now), now);
    }

    public WorkflowSchedule get(TenantId tenantId, UUID id) {
        return schedules.findById(Objects.requireNonNull(tenantId), id)
                .orElseThrow(() -> new ScheduleNotFoundException(id));
    }

    public PageResult<WorkflowSchedule> list(TenantId tenantId, int page, int size) {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        if (page < 0) throw new IllegalArgumentException("page must be at least 0");
        if (size < 1 || size > 100) throw new IllegalArgumentException("size must be between 1 and 100");
        return schedules.findAll(tenantId, page, size);
    }

    public WorkflowSchedule update(
            TenantId tenantId,
            UUID id,
            long expectedLockVersion,
            WorkflowScheduleDraft draft
    ) {
        ensureMutable(get(tenantId, id));
        validateWorkflow(tenantId, draft.workflowId());
        Instant now = clock.instant();
        return schedules.update(
                tenantId,
                id,
                expectedLockVersion,
                draft,
                calculator.nextFireAt(draft.spec(), now),
                now
        );
    }

    public WorkflowSchedule pause(TenantId tenantId, UUID id, long expectedLockVersion) {
        WorkflowSchedule current = get(tenantId, id);
        ensureMutable(current);
        return schedules.changeStatus(
                tenantId, id, expectedLockVersion, ScheduleStatus.PAUSED, current.nextFireAt(), clock.instant()
        );
    }

    public WorkflowSchedule resume(TenantId tenantId, UUID id, long expectedLockVersion) {
        WorkflowSchedule current = get(tenantId, id);
        ensureMutable(current);
        Instant now = clock.instant();
        return schedules.changeStatus(
                tenantId,
                id,
                expectedLockVersion,
                ScheduleStatus.ACTIVE,
                calculator.nextFireAt(current.spec(), now),
                now
        );
    }

    public void delete(TenantId tenantId, UUID id, long expectedLockVersion) {
        schedules.delete(tenantId, id, expectedLockVersion, clock.instant());
    }

    private void validateWorkflow(TenantId tenantId, UUID workflowId) {
        if (workflows.findById(tenantId, workflowId).isEmpty()) throw new WorkflowNotFoundException(workflowId);
        if (!workflows.hasPublishedVersion(tenantId, workflowId)) throw new WorkflowNotPublishedException(workflowId);
    }

    private static void ensureMutable(WorkflowSchedule schedule) {
        if (schedule.status() == ScheduleStatus.COMPLETED) {
            throw new ScheduleConflictException(
                    "Completed schedule " + schedule.id() + " cannot be modified"
            );
        }
    }
}
