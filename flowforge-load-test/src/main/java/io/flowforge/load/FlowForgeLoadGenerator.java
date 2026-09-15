package io.flowforge.load;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

public final class FlowForgeLoadGenerator {
    private final ObjectMapper mapper;
    private final Clock clock;

    public FlowForgeLoadGenerator() {
        this(new ObjectMapper(), Clock.systemUTC());
    }

    FlowForgeLoadGenerator(ObjectMapper mapper, Clock clock) {
        this.mapper = mapper;
        this.clock = clock;
    }

    public LoadTestReport run(WorkloadProfile profile) throws Exception {
        UUID runId = UUID.randomUUID();
        List<FlowForgeClient> clients = profile.baseUrls().stream()
                .map(url -> new FlowForgeClient(url, mapper, profile.operationTimeout().dividedBy(2)))
                .toList();
        UUID workflowId = clients.getFirst().createAndPublishWorkflow(
                runId,
                profile.fanOut(),
                profile.taskDelayMs(),
                profile.taskMaxAttempts(),
                profile.retryBackoffMs(),
                profile.workflowMaxConcurrency(),
                profile.taskMaxConcurrency()
        );
        Accumulator accumulator = new Accumulator();
        Instant startedAt = clock.instant();
        long startedNanos = System.nanoTime();
        Semaphore inFlight = new Semaphore(profile.maxInFlight());

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            long spacingNanos = Math.max(1, (long) (1_000_000_000.0 / profile.arrivalRatePerSecond()));
            for (int sequence = 0; sequence < profile.operations(); sequence++) {
                waitUntil(startedNanos + spacingNanos * sequence);
                inFlight.acquire();
                int operation = sequence;
                executor.submit(() -> {
                    try {
                        FlowForgeClient client = clients.get(operation % clients.size());
                        executeOne(client, profile, workflowId, runId, operation, accumulator);
                    } finally {
                        inFlight.release();
                    }
                });
            }
            executor.shutdown();
            long pacingSeconds = (long) Math.ceil(profile.operations() / profile.arrivalRatePerSecond());
            Duration maximumRun = profile.operationTimeout()
                    .plusSeconds(pacingSeconds)
                    .plusSeconds(10);
            if (!executor.awaitTermination(maximumRun.toMillis(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                throw new IllegalStateException("Load workers did not terminate within " + maximumRun);
            }
        }

        Instant finishedAt = clock.instant();
        double durationSeconds = Math.max(0.001, (System.nanoTime() - startedNanos) / 1_000_000_000.0);
        return accumulator.report(runId, profile, startedAt, finishedAt, durationSeconds);
    }

    private void executeOne(
            FlowForgeClient client,
            WorkloadProfile profile,
            UUID workflowId,
            UUID runId,
            int sequence,
            Accumulator accumulator
    ) {
        accumulator.attempted.increment();
        try {
            if (profile.mode() == WorkloadProfile.Mode.API) {
                executeApi(client, profile, workflowId, runId, sequence, accumulator);
            } else {
                executeScheduled(client, profile, workflowId, accumulator);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            accumulator.transportErrors.increment();
        } catch (IOException | RuntimeException failure) {
            accumulator.transportErrors.increment();
        }
    }

    private void executeApi(
            FlowForgeClient client,
            WorkloadProfile profile,
            UUID workflowId,
            UUID runId,
            int sequence,
            Accumulator accumulator
    ) throws IOException, InterruptedException {
        FlowForgeClient.TimedResponse started = client.startExecution(
                workflowId, "load-" + runId + "-" + sequence
        );
        accumulator.recordStatus(started.statusCode());
        accumulator.recordAdmissionStatus(started.statusCode());
        accumulator.startLatencies.add(started.durationNanos());
        if (started.statusCode() == 429) {
            accumulator.rejected.increment();
            return;
        }
        if (started.statusCode() != 202) {
            accumulator.unexpectedResponses.increment();
            return;
        }
        accumulator.accepted.increment();
        UUID executionId = UUID.fromString(client.json(started).get("id").asString());
        long acceptedNanos = System.nanoTime();
        Instant deadline = clock.instant().plus(profile.operationTimeout());
        while (clock.instant().isBefore(deadline)) {
            Thread.sleep(profile.pollInterval());
            FlowForgeClient.TimedResponse current;
            try {
                current = client.getExecution(executionId);
            } catch (IOException transientFailure) {
                accumulator.pollTransportErrors.increment();
                continue;
            }
            accumulator.recordStatus(current.statusCode());
            if (current.statusCode() != 200) continue;
            String status = client.json(current).get("status").asString();
            if ("SUCCEEDED".equals(status)) {
                accumulator.succeeded.increment();
                accumulator.completionLatencies.add(System.nanoTime() - acceptedNanos);
                return;
            }
            if ("FAILED".equals(status) || "CANCELLED".equals(status)) {
                accumulator.terminalFailed.increment();
                accumulator.completionLatencies.add(System.nanoTime() - acceptedNanos);
                return;
            }
        }
        accumulator.timedOut.increment();
    }

    private void executeScheduled(
            FlowForgeClient client,
            WorkloadProfile profile,
            UUID workflowId,
            Accumulator accumulator
    ) throws IOException, InterruptedException {
        Instant fireAt = clock.instant().plus(profile.scheduleLead());
        FlowForgeClient.TimedResponse created = client.createSchedule(workflowId, fireAt);
        accumulator.recordStatus(created.statusCode());
        accumulator.recordAdmissionStatus(created.statusCode());
        accumulator.startLatencies.add(created.durationNanos());
        if (created.statusCode() == 429) {
            accumulator.rejected.increment();
            return;
        }
        if (created.statusCode() != 201) {
            accumulator.unexpectedResponses.increment();
            return;
        }
        accumulator.accepted.increment();
        UUID scheduleId = UUID.fromString(client.json(created).get("id").asString());
        Instant deadline = clock.instant().plus(profile.operationTimeout());
        while (clock.instant().isBefore(deadline)) {
            Thread.sleep(profile.pollInterval());
            FlowForgeClient.TimedResponse current;
            try {
                current = client.getSchedule(scheduleId);
            } catch (IOException transientFailure) {
                accumulator.pollTransportErrors.increment();
                continue;
            }
            accumulator.recordStatus(current.statusCode());
            if (current.statusCode() != 200) continue;
            String status = client.json(current).get("status").asString();
            if ("COMPLETED".equals(status)) {
                accumulator.succeeded.increment();
                long lagNanos = Math.max(0, Duration.between(fireAt, clock.instant()).toNanos());
                accumulator.completionLatencies.add(lagNanos);
                return;
            }
            if ("DELETED".equals(status)) {
                accumulator.terminalFailed.increment();
                return;
            }
        }
        accumulator.timedOut.increment();
    }

    private static void waitUntil(long targetNanos) {
        long remaining;
        while ((remaining = targetNanos - System.nanoTime()) > 0) LockSupport.parkNanos(remaining);
    }

    private static final class Accumulator {
        private final LongAdder attempted = new LongAdder();
        private final LongAdder accepted = new LongAdder();
        private final LongAdder succeeded = new LongAdder();
        private final LongAdder terminalFailed = new LongAdder();
        private final LongAdder rejected = new LongAdder();
        private final LongAdder unexpectedResponses = new LongAdder();
        private final LongAdder unexpectedServerErrors = new LongAdder();
        private final LongAdder timedOut = new LongAdder();
        private final LongAdder transportErrors = new LongAdder();
        private final LongAdder pollTransportErrors = new LongAdder();
        private final List<Long> startLatencies = Collections.synchronizedList(new ArrayList<>());
        private final List<Long> completionLatencies = Collections.synchronizedList(new ArrayList<>());
        private final Map<Integer, LongAdder> statuses = new ConcurrentHashMap<>();
        private final Map<Integer, LongAdder> admissionStatuses = new ConcurrentHashMap<>();

        void recordStatus(int status) {
            statuses.computeIfAbsent(status, ignored -> new LongAdder()).increment();
        }

        void recordAdmissionStatus(int status) {
            admissionStatuses.computeIfAbsent(status, ignored -> new LongAdder()).increment();
            if (status >= 500 && status <= 599) unexpectedServerErrors.increment();
        }

        LoadTestReport report(
                UUID runId,
                WorkloadProfile profile,
                Instant startedAt,
                Instant finishedAt,
                double durationSeconds
        ) {
            long attemptedCount = attempted.sum();
            long acceptedCount = accepted.sum();
            long succeededCount = succeeded.sum();
            long failedCount = terminalFailed.sum();
            long rejectedCount = rejected.sum();
            long unexpectedResponseCount = unexpectedResponses.sum();
            long unexpectedServerErrorCount = unexpectedServerErrors.sum();
            long timedOutCount = timedOut.sum();
            long transportErrorCount = transportErrors.sum();
            long pollTransportErrorCount = pollTransportErrors.sum();
            double acceptanceRatio = ratio(acceptedCount, attemptedCount);
            double successRatio = ratio(succeededCount, acceptedCount);
            double errorRatio = ratio(
                    attemptedCount - succeededCount,
                    attemptedCount
            );
            LatencySummary start = LatencySummary.fromNanos(startLatencies);
            LatencySummary completion = LatencySummary.fromNanos(completionLatencies);
            WorkloadProfile.Thresholds limits = profile.thresholds();
            List<LoadTestReport.ThresholdResult> thresholds = List.of(
                    minimum("acceptanceRatio", limits.minimumAcceptanceRatio(), acceptanceRatio),
                    minimum("successRatio", limits.minimumSuccessRatio(), successRatio),
                    maximum("errorRatio", limits.maximumErrorRatio(), errorRatio),
                    maximum("start.p95Ms", limits.maximumStartP95Ms(), start.p95()),
                    maximum("completion.p95Ms", limits.maximumCompletionP95Ms(), completion.p95()),
                    maximum("admission.unexpected5xx", 0, unexpectedServerErrorCount)
            );
            Map<String, Long> statusCounts = new LinkedHashMap<>();
            statuses.entrySet().stream().sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> statusCounts.put(String.valueOf(entry.getKey()), entry.getValue().sum()));
            Map<String, Long> admissionStatusCounts = new LinkedHashMap<>();
            admissionStatuses.entrySet().stream().sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> admissionStatusCounts.put(String.valueOf(entry.getKey()), entry.getValue().sum()));
            return new LoadTestReport(
                    3, runId, profile.name(), profile.mode().name(), startedAt, finishedAt,
                    round(durationSeconds),
                    new LoadTestReport.Workload(
                            profile.baseUrls(), profile.operations(), profile.arrivalRatePerSecond(),
                            profile.maxInFlight(), profile.fanOut(), profile.fanOut() + 2, profile.taskDelayMs(),
                            profile.taskMaxAttempts(), profile.retryBackoffMs(), profile.workflowMaxConcurrency(),
                            profile.taskMaxConcurrency()
                    ),
                    new LoadTestReport.Counts(
                            attemptedCount, acceptedCount, succeededCount, failedCount,
                            rejectedCount, unexpectedResponseCount, unexpectedServerErrorCount,
                            timedOutCount, transportErrorCount,
                            pollTransportErrorCount
                    ),
                    new LoadTestReport.Rates(
                            round(attemptedCount / durationSeconds),
                            round(acceptedCount / durationSeconds),
                            round(succeededCount / durationSeconds),
                            round(acceptanceRatio), round(successRatio), round(errorRatio)
                    ),
                    new LoadTestReport.Latencies(start, completion),
                    Map.copyOf(statusCounts), Map.copyOf(admissionStatusCounts),
                    thresholds,
                    thresholds.stream().allMatch(LoadTestReport.ThresholdResult::passed)
            );
        }

        private static LoadTestReport.ThresholdResult minimum(String metric, double limit, double actual) {
            return new LoadTestReport.ThresholdResult(metric, ">=", limit, round(actual), actual >= limit);
        }

        private static LoadTestReport.ThresholdResult maximum(String metric, double limit, double actual) {
            return new LoadTestReport.ThresholdResult(metric, "<=", limit, round(actual), actual <= limit);
        }

        private static double ratio(long numerator, long denominator) {
            return denominator == 0 ? 0 : (double) numerator / denominator;
        }

        private static double round(double value) {
            return Math.round(value * 1_000_000.0) / 1_000_000.0;
        }
    }
}
