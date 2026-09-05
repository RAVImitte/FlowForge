package io.flowforge.application.schedule;

public record ScheduleMaterializationResult(int due, int pending, int skipped, int capacityDeferred) {
    public ScheduleMaterializationResult {
        if (due < 0 || pending < 0 || skipped < 0 || capacityDeferred < 0) {
            throw new IllegalArgumentException("Materialization counts must not be negative");
        }
        if (due != pending + skipped) {
            throw new IllegalArgumentException("Due count must equal pending plus skipped counts");
        }
    }

    public ScheduleMaterializationResult(int due, int pending, int skipped) {
        this(due, pending, skipped, 0);
    }

    public static ScheduleMaterializationResult empty() {
        return new ScheduleMaterializationResult(0, 0, 0, 0);
    }
}
