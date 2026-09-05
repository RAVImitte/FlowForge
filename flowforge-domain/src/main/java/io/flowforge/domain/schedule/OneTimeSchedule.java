package io.flowforge.domain.schedule;

import java.time.Instant;
import java.util.Objects;

public record OneTimeSchedule(Instant fireAt) implements ScheduleSpec {
    public OneTimeSchedule {
        Objects.requireNonNull(fireAt, "fireAt must not be null");
    }

    @Override
    public ScheduleType type() {
        return ScheduleType.ONE_TIME;
    }
}
