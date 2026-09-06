package io.flowforge.load;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

public record WorkloadProfile(
        int schemaVersion,
        String name,
        Mode mode,
        List<String> baseUrls,
        int operations,
        double arrivalRatePerSecond,
        int maxInFlight,
        int fanOut,
        long taskDelayMs,
        int taskMaxAttempts,
        long retryBackoffMs,
        Integer workflowMaxConcurrency,
        Integer taskMaxConcurrency,
        Duration pollInterval,
        Duration operationTimeout,
        Duration scheduleLead,
        Thresholds thresholds
) {
    public enum Mode { API, SCHEDULED }

    public record Thresholds(
            double minimumAcceptanceRatio,
            double minimumSuccessRatio,
            double maximumErrorRatio,
            double maximumStartP95Ms,
            double maximumCompletionP95Ms
    ) {
        public Thresholds {
            ratio(minimumAcceptanceRatio, "minimumAcceptanceRatio");
            ratio(minimumSuccessRatio, "minimumSuccessRatio");
            ratio(maximumErrorRatio, "maximumErrorRatio");
            positive(maximumStartP95Ms, "maximumStartP95Ms");
            positive(maximumCompletionP95Ms, "maximumCompletionP95Ms");
        }

        private static void ratio(double value, String name) {
            if (!Double.isFinite(value) || value < 0 || value > 1) {
                throw new IllegalArgumentException(name + " must be between 0 and 1");
            }
        }
    }

    public WorkloadProfile {
        if (schemaVersion != 1) throw new IllegalArgumentException("Unsupported profile schemaVersion: " + schemaVersion);
        if (name == null || name.isBlank()) throw new IllegalArgumentException("name is required");
        if (mode == null) throw new IllegalArgumentException("mode is required");
        if (baseUrls == null || baseUrls.isEmpty()) throw new IllegalArgumentException("baseUrls are required");
        for (String baseUrl : baseUrls) {
            if (baseUrl == null || !baseUrl.matches("https?://[^\\s/]+(?::[0-9]+)?/?")) {
                throw new IllegalArgumentException("Each baseUrl must be an HTTP origin without a path");
            }
        }
        if (operations < 1 || operations > 1_000_000) {
            throw new IllegalArgumentException("operations must be between 1 and 1000000");
        }
        positive(arrivalRatePerSecond, "arrivalRatePerSecond");
        if (maxInFlight < 1 || maxInFlight > 100_000) {
            throw new IllegalArgumentException("maxInFlight must be between 1 and 100000");
        }
        if (fanOut < 1 || fanOut > 998) throw new IllegalArgumentException("fanOut must be between 1 and 998");
        if (taskDelayMs < 0 || taskDelayMs > 60_000) {
            throw new IllegalArgumentException("taskDelayMs must be between 0 and 60000");
        }
        if (taskMaxAttempts < 1 || taskMaxAttempts > 100) {
            throw new IllegalArgumentException("taskMaxAttempts must be between 1 and 100");
        }
        if (retryBackoffMs < 0 || retryBackoffMs > 60_000) {
            throw new IllegalArgumentException("retryBackoffMs must be between 0 and 60000");
        }
        optionalLimit(workflowMaxConcurrency, "workflowMaxConcurrency");
        optionalLimit(taskMaxConcurrency, "taskMaxConcurrency");
        positive(pollInterval, "pollInterval");
        positive(operationTimeout, "operationTimeout");
        if (scheduleLead.isNegative() || scheduleLead.isZero()) {
            throw new IllegalArgumentException("scheduleLead must be positive");
        }
        if (thresholds == null) throw new IllegalArgumentException("thresholds are required");
        baseUrls = baseUrls.stream().map(url -> url.replaceFirst("/$", "")).distinct().toList();
    }

    public static WorkloadProfile load(Path path, ObjectMapper mapper) throws IOException {
        JsonNode root = mapper.readTree(Files.readString(path));
        JsonNode thresholdNode = required(root, "thresholds");
        return new WorkloadProfile(
                integer(root, "schemaVersion"),
                text(root, "name"),
                Mode.valueOf(text(root, "mode").toUpperCase()),
                urls(root),
                integer(root, "operations"),
                decimal(root, "arrivalRatePerSecond"),
                integer(root, "maxInFlight"),
                integer(root, "fanOut"),
                number(root, "taskDelayMs"),
                optionalInteger(root, "taskMaxAttempts", 1),
                optionalNumber(root, "retryBackoffMs", 0),
                optionalInteger(root, "workflowMaxConcurrency"),
                optionalInteger(root, "taskMaxConcurrency"),
                Duration.ofMillis(number(root, "pollIntervalMs")),
                Duration.ofSeconds(number(root, "operationTimeoutSeconds")),
                Duration.ofSeconds(number(root, "scheduleLeadSeconds")),
                new Thresholds(
                        decimal(thresholdNode, "minimumAcceptanceRatio"),
                        decimal(thresholdNode, "minimumSuccessRatio"),
                        decimal(thresholdNode, "maximumErrorRatio"),
                        decimal(thresholdNode, "maximumStartP95Ms"),
                        decimal(thresholdNode, "maximumCompletionP95Ms")
                )
        );
    }

    public String primaryBaseUrl() {
        return baseUrls.getFirst();
    }

    private static List<String> urls(JsonNode root) {
        JsonNode values = root.get("baseUrls");
        if (values != null && values.isArray()) {
            List<String> urls = new java.util.ArrayList<>();
            values.forEach(value -> urls.add(value.asString()));
            return urls;
        }
        return List.of(text(root, "baseUrl"));
    }

    private static JsonNode required(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) throw new IllegalArgumentException(field + " is required");
        return value;
    }

    private static String text(JsonNode node, String field) {
        String value = required(node, field).asString();
        if (value.isBlank()) throw new IllegalArgumentException(field + " is required");
        return value;
    }

    private static int integer(JsonNode node, String field) {
        return required(node, field).asInt();
    }

    private static int optionalInteger(JsonNode node, String field, int defaultValue) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? defaultValue : value.asInt();
    }

    private static Integer optionalInteger(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asInt();
    }

    private static long number(JsonNode node, String field) {
        return required(node, field).asLong();
    }

    private static long optionalNumber(JsonNode node, String field, long defaultValue) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? defaultValue : value.asLong();
    }

    private static double decimal(JsonNode node, String field) {
        return required(node, field).asDouble();
    }

    private static void positive(double value, String name) {
        if (!Double.isFinite(value) || value <= 0) throw new IllegalArgumentException(name + " must be positive");
    }

    private static void positive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static void optionalLimit(Integer value, String name) {
        if (value != null && (value < 1 || value > 100_000)) {
            throw new IllegalArgumentException(name + " must be between 1 and 100000");
        }
    }
}
