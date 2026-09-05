package io.flowforge.domain.schedule;

public sealed interface ScheduleSpec permits OneTimeSchedule, CronSchedule {
    ScheduleType type();
}
