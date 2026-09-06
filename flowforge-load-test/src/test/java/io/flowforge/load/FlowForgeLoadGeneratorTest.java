package io.flowforge.load;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class FlowForgeLoadGeneratorTest {
    private static final UUID WORKFLOW_ID = UUID.fromString("10000000-0000-0000-0000-000000000001");
    private final ObjectMapper mapper = new ObjectMapper();
    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) server.stop(0);
    }

    @Test
    void runsPacedFanOutWorkflowsAndEvaluatesThresholds() throws Exception {
        AtomicInteger starts = new AtomicInteger();
        AtomicReference<JsonNode> workflowRequest = new AtomicReference<>();
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/api/v1/workflows", exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/api/v1/workflows")) {
                workflowRequest.set(mapper.readTree(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
                respond(exchange, 201, "{\"id\":\"" + WORKFLOW_ID + "\",\"lockVersion\":0}");
            } else if (path.endsWith("/publish")) {
                respond(exchange, 200, "{\"id\":\"" + WORKFLOW_ID + "\"}");
            } else if (path.endsWith("/executions")) {
                int sequence = starts.incrementAndGet();
                respond(exchange, 202, "{\"id\":\"20000000-0000-0000-0000-" + String.format("%012d", sequence) + "\"}");
            } else {
                respond(exchange, 404, "{}");
            }
        });
        server.createContext("/api/v1/executions", exchange ->
                respond(exchange, 200, "{\"status\":\"SUCCEEDED\"}"));
        server.start();

        WorkloadProfile profile = profile(4, server.getAddress().getPort(), 3);
        LoadTestReport report = new FlowForgeLoadGenerator().run(profile);

        assertThat(report.passed()).isTrue();
        assertThat(report.counts().attempted()).isEqualTo(4);
        assertThat(report.counts().accepted()).isEqualTo(4);
        assertThat(report.counts().succeeded()).isEqualTo(4);
        assertThat(report.counts().unexpectedResponses()).isZero();
        assertThat(report.rates().acceptanceRatio()).isEqualTo(1.0);
        assertThat(report.httpStatuses()).containsEntry("202", 4L).containsEntry("200", 4L);
        assertThat(workflowRequest.get().path("tasks").size()).isEqualTo(5);
        assertThat(workflowRequest.get().path("dependencies").size()).isEqualTo(6);
        assertThat(workflowRequest.get().toString()).contains("START", "FAN_001", "JOIN", "durationMs");
        assertThat(workflowRequest.get().path("tasks").get(0).path("reliabilityPolicy").path("maxAttempts").asInt())
                .isEqualTo(3);
        assertThat(workflowRequest.get().path("maxConcurrentExecutions").asInt()).isEqualTo(10);
        assertThat(workflowRequest.get().path("tasks").get(0).path("maxConcurrency").asInt()).isEqualTo(2);
    }

    @Test
    void classifiesUnexpectedStartResponsesSeparatelyFromTransportFailures() throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/api/v1/workflows", exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/api/v1/workflows")) {
                respond(exchange, 201, "{\"id\":\"" + WORKFLOW_ID + "\",\"lockVersion\":0}");
            } else if (path.endsWith("/publish")) {
                respond(exchange, 200, "{\"id\":\"" + WORKFLOW_ID + "\"}");
            } else if (path.endsWith("/executions")) {
                respond(exchange, 500, "{\"error\":\"saturated\"}");
            } else {
                respond(exchange, 404, "{}");
            }
        });
        server.start();

        LoadTestReport report = new FlowForgeLoadGenerator().run(profile(3, server.getAddress().getPort()));

        assertThat(report.counts().attempted()).isEqualTo(3);
        assertThat(report.counts().accepted()).isZero();
        assertThat(report.counts().unexpectedResponses()).isEqualTo(3);
        assertThat(report.counts().transportErrors()).isZero();
        assertThat(report.httpStatuses()).containsEntry("500", 3L);
        assertThat(report.counts().attempted()).isEqualTo(
                report.counts().accepted()
                        + report.counts().rejected()
                        + report.counts().unexpectedResponses()
                        + report.counts().transportErrors()
        );
    }

    @Test
    void retriesTransientPollTransportFailuresForAcceptedExecutions() throws Exception {
        AtomicInteger polls = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/api/v1/workflows", exchange -> {
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/api/v1/workflows")) {
                respond(exchange, 201, "{\"id\":\"" + WORKFLOW_ID + "\",\"lockVersion\":0}");
            } else if (path.endsWith("/publish")) {
                respond(exchange, 200, "{\"id\":\"" + WORKFLOW_ID + "\"}");
            } else if (path.endsWith("/executions")) {
                respond(exchange, 202, "{\"id\":\"20000000-0000-0000-0000-000000000001\"}");
            } else {
                respond(exchange, 404, "{}");
            }
        });
        server.createContext("/api/v1/executions", exchange -> {
            if (polls.getAndIncrement() == 0) {
                java.util.concurrent.locks.LockSupport.parkNanos(Duration.ofMillis(1_100).toNanos());
            }
            respond(exchange, 200, "{\"status\":\"SUCCEEDED\"}");
        });
        server.start();

        LoadTestReport report = new FlowForgeLoadGenerator().run(profile(1, server.getAddress().getPort()));

        assertThat(report.schemaVersion()).isEqualTo(2);
        assertThat(report.counts().succeeded()).isEqualTo(1);
        assertThat(report.counts().transportErrors()).isZero();
        assertThat(report.counts().pollTransportErrors()).isEqualTo(1);
        assertThat(report.passed()).isTrue();
    }

    private static WorkloadProfile profile(int operations, int port) {
        return profile(operations, port, 1);
    }

    private static WorkloadProfile profile(int operations, int port, int taskMaxAttempts) {
        return new WorkloadProfile(
                1, "contract", WorkloadProfile.Mode.API, java.util.List.of("http://localhost:" + port),
                operations, 1_000, operations, 3, 0, taskMaxAttempts, taskMaxAttempts > 1 ? 500 : 0,
                10, 2,
                Duration.ofMillis(1), Duration.ofSeconds(2), Duration.ofSeconds(1),
                new WorkloadProfile.Thresholds(1, 1, 0, 1_000, 5_000)
        );
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
