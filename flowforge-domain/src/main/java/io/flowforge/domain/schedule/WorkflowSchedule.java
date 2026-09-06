package io.flowforge.domain.schedule;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record WorkflowSchedule(
        UUID id,
        TenantId tenantId,
        UUID workflowId,
        ScheduleSpec spec,
        MisfirePolicy misfirePolicy,
        ScheduleStatus status,
        Instant nextFireAt,
        long lockVersion,
        Instant createdAt,
        Instant updatedAt
) {
    public WorkflowSchedule {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(workflowId, "workflowId must not be null");
        Objects.requireNonNull(spec, "spec must not be null");
        Objects.requireNonNull(misfirePolicy, "misfirePolicy must not be null");
        Objects.requireNonNull(status, "status must not be null");
        if (status == ScheduleStatus.COMPLETED && nextFireAt != null) {
            throw new IllegalArgumentException("A completed schedule must not have a next fire time");
        }
        if (status != ScheduleStatus.COMPLETED && nextFireAt == null) {
            throw new IllegalArgumentException("A non-completed schedule must have a next fire time");
        }
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        if (lockVersion < 0) throw new IllegalArgumentException("lockVersion must not be negative");
        if (updatedAt.isBefore(createdAt)) {
            throw new IllegalArgumentException("updatedAt must not be before createdAt");
        }
    }
}
