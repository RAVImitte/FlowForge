package io.flowforge.load;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

final class FlowForgeClient {
    private final String baseUrl;
    private final ObjectMapper mapper;
    private final HttpClient http;
    private final Duration requestTimeout;

    FlowForgeClient(String baseUrl, ObjectMapper mapper, Duration requestTimeout) {
        this.baseUrl = baseUrl;
        this.mapper = mapper;
        this.requestTimeout = requestTimeout;
        this.http = HttpClient.newBuilder()
                .connectTimeout(requestTimeout)
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    UUID createAndPublishWorkflow(
            UUID runId,
            int fanOut,
            long taskDelayMs,
            int taskMaxAttempts,
            long retryBackoffMs,
            Integer workflowMaxConcurrency,
            Integer taskMaxConcurrency
    ) throws IOException, InterruptedException {
        List<Map<String, Object>> tasks = new ArrayList<>();
        List<Map<String, String>> dependencies = new ArrayList<>();
        tasks.add(task("START", "NOOP", Map.of(), taskMaxAttempts, retryBackoffMs, taskMaxConcurrency));
        for (int index = 1; index <= fanOut; index++) {
            String key = "FAN_" + String.format("%03d", index);
            tasks.add(task(key, "DELAY", Map.of("durationMs", taskDelayMs), taskMaxAttempts, retryBackoffMs, taskMaxConcurrency));
            dependencies.add(Map.of("taskKey", key, "dependsOnTaskKey", "START"));
        }
        tasks.add(task("JOIN", "NOOP", Map.of(), taskMaxAttempts, retryBackoffMs, taskMaxConcurrency));
        for (int index = 1; index <= fanOut; index++) {
            dependencies.add(Map.of(
                    "taskKey", "JOIN",
                    "dependsOnTaskKey", "FAN_" + String.format("%03d", index)
            ));
        }
        Map<String, Object> workflow = new LinkedHashMap<>();
        workflow.put("name", "Load test " + runId);
        workflow.put("description", "Generated fan-out/fan-in workload for bounded capacity testing");
        if (workflowMaxConcurrency != null) workflow.put("maxConcurrentExecutions", workflowMaxConcurrency);
        workflow.put("tasks", tasks);
        workflow.put("dependencies", dependencies);

        TimedResponse created = post("/api/v1/workflows", workflow, Map.of());
        requireStatus(created, 201, "create workflow");
        JsonNode createdBody = mapper.readTree(created.body());
        UUID workflowId = UUID.fromString(createdBody.get("id").asString());
        long lockVersion = createdBody.get("lockVersion").asLong();
        TimedResponse published = post(
                "/api/v1/workflows/" + workflowId + "/publish",
                null,
                Map.of("If-Match", "\"" + lockVersion + "\"")
        );
        requireStatus(published, 200, "publish workflow");
        return workflowId;
    }

    TimedResponse startExecution(UUID workflowId, String idempotencyKey) throws IOException, InterruptedException {
        return post(
                "/api/v1/workflows/" + workflowId + "/executions",
                null,
                Map.of("Idempotency-Key", idempotencyKey)
        );
    }

    TimedResponse createSchedule(UUID workflowId, Instant fireAt) throws IOException, InterruptedException {
        return post("/api/v1/schedules", Map.of(
                "workflowId", workflowId.toString(),
                "type", "ONE_TIME",
                "fireAt", fireAt.toString(),
                "misfirePolicy", "FIRE_ONCE"
        ), Map.of());
    }

    TimedResponse getExecution(UUID executionId) throws IOException, InterruptedException {
        return get("/api/v1/executions/" + executionId);
    }

    TimedResponse getSchedule(UUID scheduleId) throws IOException, InterruptedException {
        return get("/api/v1/schedules/" + scheduleId);
    }

    JsonNode json(TimedResponse response) throws IOException {
        return mapper.readTree(response.body());
    }

    private TimedResponse get(String path) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(requestTimeout)
                .header("Accept", "application/json")
                .GET()
                .build();
        return send(request);
    }

    private TimedResponse post(String path, Object body, Map<String, String> headers)
            throws IOException, InterruptedException {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(requestTimeout)
                .header("Accept", "application/json")
                .header("Content-Type", "application/json");
        headers.forEach(request::header);
        request.POST(HttpRequest.BodyPublishers.ofString(body == null ? "" : mapper.writeValueAsString(body)));
        return send(request.build());
    }

    private TimedResponse send(HttpRequest request) throws IOException, InterruptedException {
        long started = System.nanoTime();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        return new TimedResponse(response.statusCode(), response.body(), System.nanoTime() - started);
    }

    private static Map<String, Object> task(
            String key,
            String type,
            Map<String, Object> configuration,
            int maxAttempts,
            long retryBackoffMs,
            Integer maxConcurrency
    ) {
        Map<String, Object> task = new LinkedHashMap<>();
        task.put("key", key);
        task.put("name", key);
        task.put("type", type);
        task.put("configuration", configuration);
        if (maxConcurrency != null) task.put("maxConcurrency", maxConcurrency);
        if (maxAttempts > 1) {
            task.put("reliabilityPolicy", Map.of(
                    "maxAttempts", maxAttempts,
                    "initialBackoffMs", retryBackoffMs,
                    "backoffMultiplier", 1.0,
                    "maxBackoffMs", retryBackoffMs,
                    "jitterFactor", 0.0
            ));
        }
        return task;
    }

    private static void requireStatus(TimedResponse response, int expected, String operation) {
        if (response.statusCode() != expected) {
            String body = response.body().length() > 500 ? response.body().substring(0, 500) : response.body();
            throw new IllegalStateException(operation + " returned HTTP " + response.statusCode() + ": " + body);
        }
    }

    record TimedResponse(int statusCode, String body, long durationNanos) {}
}
