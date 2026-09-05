package io.flowforge.controlplane.adapter.out.schedule;

import io.flowforge.application.schedule.ScheduleCalculator;
import io.flowforge.domain.schedule.CronSchedule;
import io.flowforge.domain.schedule.OneTimeSchedule;
import io.flowforge.domain.schedule.ScheduleSpec;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Objects;

@Component
public class SpringScheduleCalculator implements ScheduleCalculator {
    @Override
    public Instant nextFireAt(ScheduleSpec spec, Instant after) {
        Objects.requireNonNull(spec, "spec must not be null");
        Objects.requireNonNull(after, "after must not be null");
        if (spec instanceof OneTimeSchedule oneTime) {
            if (!oneTime.fireAt().isAfter(after)) {
                throw new IllegalArgumentException("fireAt must be in the future");
            }
            return oneTime.fireAt();
        }
        CronSchedule cron = (CronSchedule) spec;
        CronExpression expression;
        try {
            expression = CronExpression.parse(cron.expression());
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("Invalid cronExpression: " + invalid.getMessage(), invalid);
        }
        ZonedDateTime next = expression.next(ZonedDateTime.ofInstant(after, cron.timeZone()));
        if (next == null) throw new IllegalArgumentException("cronExpression has no future occurrence");
        return next.toInstant();
    }
}
