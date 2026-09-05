package io.flowforge.application.schedule;

public record ScheduleFireRunResult(
        int materialized,
        int skipped,
        int claimed,
        int started,
        int failed,
        int released,
        int staleAcknowledgements
) {
}
