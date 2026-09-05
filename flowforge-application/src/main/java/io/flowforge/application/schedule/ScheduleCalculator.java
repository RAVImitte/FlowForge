package io.flowforge.application.schedule;

import io.flowforge.domain.schedule.ScheduleSpec;

import java.time.Instant;

public interface ScheduleCalculator {
    Instant nextFireAt(ScheduleSpec spec, Instant after);
}
