package io.flowforge.application.schedule;

import java.util.UUID;

public final class ScheduleNotFoundException extends RuntimeException {
    public ScheduleNotFoundException(UUID id) {
        super("Schedule not found: " + id);
    }
}
