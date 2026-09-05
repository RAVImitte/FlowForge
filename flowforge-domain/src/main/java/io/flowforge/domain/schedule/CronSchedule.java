package io.flowforge.domain.schedule;

import java.time.ZoneId;
import java.util.Objects;

public record CronSchedule(String expression, ZoneId timeZone) implements ScheduleSpec {
    private static final int MAX_EXPRESSION_LENGTH = 200;

    public CronSchedule {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("cronExpression must not be blank");
        }
        expression = expression.strip();
        if (expression.length() > MAX_EXPRESSION_LENGTH) {
            throw new IllegalArgumentException("cronExpression cannot exceed 200 characters");
        }
        Objects.requireNonNull(timeZone, "timeZone must not be null");
    }

    @Override
    public ScheduleType type() {
        return ScheduleType.CRON;
    }
}
