package io.flowforge.application.schedule;

import io.flowforge.application.workflow.PageResult;
import io.flowforge.domain.schedule.ScheduleStatus;
import io.flowforge.domain.schedule.WorkflowSchedule;
import io.flowforge.domain.schedule.WorkflowScheduleDraft;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface ScheduleRepository {
    WorkflowSchedule create(WorkflowScheduleDraft draft, Instant nextFireAt, Instant now);

    Optional<WorkflowSchedule> findById(UUID id);

    PageResult<WorkflowSchedule> findAll(int page, int size);

    WorkflowSchedule update(
            UUID id,
            long expectedLockVersion,
            WorkflowScheduleDraft draft,
            Instant nextFireAt,
            Instant now
    );

    WorkflowSchedule changeStatus(
            UUID id,
            long expectedLockVersion,
            ScheduleStatus status,
            Instant nextFireAt,
            Instant now
    );

    void delete(UUID id, long expectedLockVersion, Instant now);
}
