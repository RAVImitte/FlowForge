package io.flowforge.application.schedule;

import io.flowforge.application.workflow.PageResult;
import io.flowforge.domain.schedule.ScheduleStatus;
import io.flowforge.domain.schedule.WorkflowSchedule;
import io.flowforge.domain.schedule.WorkflowScheduleDraft;
import io.flowforge.domain.tenancy.TenantId;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface ScheduleRepository {
    WorkflowSchedule create(TenantId tenantId, WorkflowScheduleDraft draft, Instant nextFireAt, Instant now);

    Optional<WorkflowSchedule> findById(TenantId tenantId, UUID id);

    PageResult<WorkflowSchedule> findAll(TenantId tenantId, int page, int size);

    WorkflowSchedule update(
            TenantId tenantId,
            UUID id,
            long expectedLockVersion,
            WorkflowScheduleDraft draft,
            Instant nextFireAt,
            Instant now
    );

    WorkflowSchedule changeStatus(
            TenantId tenantId,
            UUID id,
            long expectedLockVersion,
            ScheduleStatus status,
            Instant nextFireAt,
            Instant now
    );

    void delete(TenantId tenantId, UUID id, long expectedLockVersion, Instant now);
}
