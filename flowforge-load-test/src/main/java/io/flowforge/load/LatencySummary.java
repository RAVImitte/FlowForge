package io.flowforge.load;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public record LatencySummary(long count, double p50, double p95, double p99, double max) {
    static LatencySummary fromNanos(List<Long> nanos) {
        if (nanos.isEmpty()) return new LatencySummary(0, 0, 0, 0, 0);
        List<Long> sorted = new ArrayList<>(nanos);
        Collections.sort(sorted);
        return new LatencySummary(
                sorted.size(),
                milliseconds(percentile(sorted, 0.50)),
                milliseconds(percentile(sorted, 0.95)),
                milliseconds(percentile(sorted, 0.99)),
                milliseconds(sorted.getLast())
        );
    }

    private static long percentile(List<Long> sorted, double quantile) {
        int index = (int) Math.ceil(quantile * sorted.size()) - 1;
        return sorted.get(Math.max(0, index));
    }

    private static double milliseconds(long nanos) {
        return Math.round((nanos / 1_000_000.0) * 1000.0) / 1000.0;
    }
}
