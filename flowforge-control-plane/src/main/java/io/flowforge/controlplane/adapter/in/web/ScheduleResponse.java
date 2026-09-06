package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.domain.schedule.CronSchedule;
import io.flowforge.domain.schedule.OneTimeSchedule;
import io.flowforge.domain.schedule.WorkflowSchedule;

import java.time.Instant;
import java.util.UUID;

public record ScheduleResponse(
        UUID id,
        String tenantId,
        UUID workflowId,
        String type,
        Instant fireAt,
        String cronExpression,
        String timeZone,
        String misfirePolicy,
        String status,
        Instant nextFireAt,
        long lockVersion,
        Instant createdAt,
        Instant updatedAt
) {
    static ScheduleResponse from(WorkflowSchedule schedule) {
        Instant fireAt = schedule.spec() instanceof OneTimeSchedule oneTime ? oneTime.fireAt() : null;
        String cronExpression = schedule.spec() instanceof CronSchedule cron ? cron.expression() : null;
        String timeZone = schedule.spec() instanceof CronSchedule cron ? cron.timeZone().getId() : null;
        return new ScheduleResponse(
                schedule.id(),
                schedule.tenantId().value(),
                schedule.workflowId(),
                schedule.spec().type().name(),
                fireAt,
                cronExpression,
                timeZone,
                schedule.misfirePolicy().name(),
                schedule.status().name(),
                schedule.nextFireAt(),
                schedule.lockVersion(),
                schedule.createdAt(),
                schedule.updatedAt()
        );
    }
}
