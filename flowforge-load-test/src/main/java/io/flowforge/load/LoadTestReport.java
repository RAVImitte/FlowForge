package io.flowforge.load;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public record LoadTestReport(
        int schemaVersion,
        UUID runId,
        String profile,
        String mode,
        Instant startedAt,
        Instant finishedAt,
        double durationSeconds,
        Workload workload,
        Counts counts,
        Rates rates,
        Latencies latencyMs,
        Map<String, Long> httpStatuses,
        Map<String, Long> admissionHttpStatuses,
        List<ThresholdResult> thresholds,
        boolean passed
) {
    public record Workload(
            List<String> baseUrls,
            int operations,
            double arrivalRatePerSecond,
            int maxInFlight,
            int fanOut,
            int tasksPerWorkflow,
            long taskDelayMs,
            int taskMaxAttempts,
            long retryBackoffMs,
            Integer workflowMaxConcurrency,
            Integer taskMaxConcurrency
    ) {}

    public record Counts(
            long attempted,
            long accepted,
            long succeeded,
            long terminalFailed,
            long rejected,
            long unexpectedResponses,
            long unexpectedServerErrors,
            long timedOut,
            long transportErrors,
            long pollTransportErrors
    ) {}

    public record Rates(
            double attemptedPerSecond,
            double acceptedPerSecond,
            double completedPerSecond,
            double acceptanceRatio,
            double successRatio,
            double errorRatio
    ) {}

    public record Latencies(LatencySummary start, LatencySummary completion) {}

    public record ThresholdResult(String metric, String operator, double limit, double actual, boolean passed) {}
}
