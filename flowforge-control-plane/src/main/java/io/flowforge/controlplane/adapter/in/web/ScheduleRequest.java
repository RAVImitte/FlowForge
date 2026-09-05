package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.domain.schedule.CronSchedule;
import io.flowforge.domain.schedule.MisfirePolicy;
import io.flowforge.domain.schedule.OneTimeSchedule;
import io.flowforge.domain.schedule.ScheduleSpec;
import io.flowforge.domain.schedule.ScheduleType;
import io.flowforge.domain.schedule.WorkflowScheduleDraft;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.UUID;

public record ScheduleRequest(
        @NotNull UUID workflowId,
        @NotNull ScheduleType type,
        Instant fireAt,
        @Size(max = 200) String cronExpression,
        @Size(max = 100) String timeZone,
        MisfirePolicy misfirePolicy
) {
    WorkflowScheduleDraft toDraft() {
        ScheduleSpec spec = switch (type) {
            case ONE_TIME -> {
                if (fireAt == null) throw new IllegalArgumentException("fireAt is required for ONE_TIME schedules");
                if (cronExpression != null || timeZone != null) {
                    throw new IllegalArgumentException(
                            "cronExpression and timeZone are not allowed for ONE_TIME schedules"
                    );
                }
                yield new OneTimeSchedule(fireAt);
            }
            case CRON -> {
                if (fireAt != null) throw new IllegalArgumentException("fireAt is not allowed for CRON schedules");
                if (cronExpression == null || cronExpression.isBlank()) {
                    throw new IllegalArgumentException("cronExpression is required for CRON schedules");
                }
                if (timeZone == null || timeZone.isBlank()) {
                    throw new IllegalArgumentException("timeZone is required for CRON schedules");
                }
                try {
                    yield new CronSchedule(cronExpression, ZoneId.of(timeZone));
                } catch (ZoneRulesException invalidZone) {
                    throw new IllegalArgumentException("Invalid timeZone: " + timeZone, invalidZone);
                }
            }
        };
        return new WorkflowScheduleDraft(workflowId, spec, misfirePolicy);
    }
}
