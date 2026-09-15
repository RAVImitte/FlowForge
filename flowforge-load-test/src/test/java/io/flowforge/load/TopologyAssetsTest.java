package io.flowforge.load;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TopologyAssetsTest {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Path repositoryRoot = findRepositoryRoot();

    @Test
    void topologyCatalogIsVersionedAndResourceSafe() throws Exception {
        Path topologyDirectory = repositoryRoot.resolve("load-testing/topologies");
        List<Path> files;
        try (var paths = Files.list(topologyDirectory)) {
            files = paths.filter(path -> path.toString().endsWith(".json")).sorted().toList();
        }

        assertThat(files).extracting(path -> stripExtension(path.getFileName().toString()))
                .containsExactly(
                        "balanced-2x2-6p", "single-6p", "single-pool2-6p",
                        "workers-2-6p", "workers-2-c4-6p", "workers-4-c3-12p"
                );

        for (Path file : files) {
            JsonNode topology = objectMapper.readTree(Files.readString(file));
            assertThat(topology.get("schemaVersion").asInt()).isEqualTo(1);
            assertThat(topology.get("name").asString()).isEqualTo(stripExtension(file.getFileName().toString()));
            assertThat(topology.get("kafkaPartitions").asInt()).isPositive();
            assertThat(topology.get("probeIntervalSeconds").asInt()).isPositive();
            assertThat(topology.get("startupTimeoutSeconds").asInt()).isGreaterThanOrEqualTo(30);
            assertThat(topology.get("postRunDrainSeconds").asInt()).isNotNegative();

            JsonNode controlPlane = topology.get("controlPlane");
            JsonNode worker = topology.get("worker");
            assertThat(controlPlane.get("databasePoolSize").asInt()).isPositive();
            assertThat(controlPlane.get("maxHeapMb").asInt()).isPositive();
            assertThat(worker.get("databasePoolSize").asInt()).isPositive();
            assertThat(worker.get("concurrency").asInt()).isPositive();
            assertThat(worker.get("maxHeapMb").asInt()).isPositive();

            Set<Integer> ports = new HashSet<>();
            assertPorts(controlPlane.get("ports"), ports);
            assertPorts(worker.get("ports"), ports);
        }
    }

    @Test
    void topologyRunnerCapturesEvidenceAndAlwaysCleansUpOwnedProcesses() throws Exception {
        String script = Files.readString(repositoryRoot.resolve("scripts/run-load-topology.ps1"));
        String processWrapper = Files.readString(repositoryRoot.resolve("scripts/invoke-load-generator.ps1"));
        String resultIgnores = Files.readString(repositoryRoot.resolve("load-testing/results/.gitignore"));

        assertThat(script)
                .contains("[switch]$ResetData")
                .contains("if ($ResetData)")
                .contains("baseUrls")
                .contains("flowforge_worker_commands_completed_total")
                .contains("flowforge_results_consumed_total")
                .contains("kafka-consumer-groups.sh")
                .contains("--describe --all-groups")
                .contains("timeout 8s")
                .contains("kafkaAssignments")
                .contains("retriesScheduled")
                .contains("taskTimeouts")
                .contains("flowforge.task.heartbeats.v1")
                .contains("/actuator/health/readiness")
                .contains("Wait-Applications $controlApplications")
                .contains("Assert-KafkaPartitions")
                .contains("Wait-Applications $workerApplications")
                .contains("pg_stat_activity")
                .contains("durableExecutionCounts")
                .contains("durableFailureCauses")
                .contains("kafkaProbeError")
                .contains("PostgreSQL probe unavailable")
                .contains("correctnessPassed")
                .contains("finally {")
                .contains("Stop-Applications $applications")
                .contains("Stop-Process -Id $loadProcess.Id -Force")
                .contains("[System.Text.UTF8Encoding]::new($false)");
        assertThat(processWrapper)
                .contains("$LASTEXITCODE")
                .contains("$ExitCodeFile")
                .contains("exit $code");
        assertThat(resultIgnores).contains("*.json", "*.log", "*.exit-code", "!.gitkeep");
    }

    @Test
    void capacityMatrixReferencesVersionedProfilesAndTopologies() throws Exception {
        JsonNode matrix = objectMapper.readTree(Files.readString(repositoryRoot.resolve("load-testing/capacity-matrix.json")));
        assertThat(matrix.get("schemaVersion").asInt()).isEqualTo(1);
        assertThat(matrix.get("name").asString()).isEqualTo("phase-6-capacity-matrix");

        Set<String> ids = new HashSet<>();
        Set<String> categories = new HashSet<>();
        matrix.get("scenarios").forEach(scenario -> {
            String id = scenario.get("id").asString();
            String topology = scenario.get("topology").asString();
            String profile = scenario.get("profile").asString();
            assertThat(ids.add(id)).as("scenario id %s must be unique", id).isTrue();
            categories.add(scenario.get("category").asString());
            assertThat(repositoryRoot.resolve("load-testing/topologies/" + topology + ".json")).isRegularFile();
            assertThat(repositoryRoot.resolve("load-testing/profiles/" + profile + ".json")).isRegularFile();
        });

        assertThat(ids).containsExactlyInAnyOrder(
                "baseline-smoke", "baseline", "horizontal-workers", "horizontal-balanced-smoke",
                "horizontal-balanced", "pool-bound", "partition-bound", "overload", "scheduled",
                "scheduled-safe", "soak"
        );
        assertThat(categories).containsExactlyInAnyOrder(
                "safe-envelope", "baseline", "horizontal-scale", "database-pool",
                "kafka-partitions", "overload", "scheduling", "soak"
        );
        assertThat(requiredScenarioIds(matrix, true)).containsExactlyInAnyOrder(
                "baseline-smoke", "horizontal-balanced-smoke", "scheduled-safe", "soak"
        );
        assertThat(requiredScenarioIds(matrix, false)).containsExactlyInAnyOrder(
                "baseline", "horizontal-workers", "horizontal-balanced", "pool-bound", "partition-bound",
                "overload", "scheduled"
        );

        String runner = Files.readString(repositoryRoot.resolve("scripts/run-capacity-matrix.ps1"));
        assertThat(runner)
                .contains("requiredFailureCount")
                .contains("redacted-local-rancher-host")
                .contains("run-load-topology.ps1")
                .contains("-SkipBuild -ResetData")
                .contains("[switch]$SummarizeOnly")
                .contains("durableFailureCauses")
                .contains("docs/performance/capacity-matrix-summary.json");
    }

    @Test
    void boundedFaultPlansAreVersionedRestorableAndBackedByCuratedEvidence() throws Exception {
        Path faultDirectory = repositoryRoot.resolve("load-testing/faults");
        List<Path> files;
        try (var paths = Files.list(faultDirectory)) {
            files = paths.filter(path -> path.toString().endsWith(".json")).sorted().toList();
        }
        assertThat(files).extracting(path -> stripExtension(path.getFileName().toString()))
                .containsExactly("kafka-pause", "postgres-pause", "redis-loss", "telemetry-pause");

        for (Path file : files) {
            JsonNode plan = objectMapper.readTree(Files.readString(file));
            assertThat(plan.path("schemaVersion").asInt()).isEqualTo(1);
            assertThat(plan.path("name").asString()).isEqualTo(stripExtension(file.getFileName().toString()));
            assertThat(plan.path("actions").size()).isBetween(1, 4);
            plan.path("actions").forEach(action -> {
                assertThat(action.path("service").asString())
                        .isIn("postgres", "kafka", "redis", "prometheus", "otel-collector");
                assertThat(action.path("operation").asString()).isIn("pause", "flush");
                assertThat(action.path("delaySeconds").asInt()).isBetween(1, 30);
                if (action.path("operation").asString().equals("flush")) {
                    assertThat(action.path("service").asString()).isEqualTo("redis");
                    assertThat(action.path("durationSeconds").asInt()).isZero();
                } else {
                    assertThat(action.path("durationSeconds").asInt()).isBetween(1, 30);
                }
            });
            assertThat(plan.path("thresholds").path("maximumRecoverySeconds").asInt()).isPositive();
            assertThat(plan.path("thresholds").path("maximumSuppressedCommandDuplicates").asInt())
                    .isNotNegative();
            assertThat(plan.path("thresholds").path("maximumDurableFailures").asInt()).isNotNegative();
        }

        String runner = Files.readString(repositoryRoot.resolve("scripts/run-load-topology.ps1"));
        String injector = Files.readString(repositoryRoot.resolve("scripts/invoke-dependency-fault.ps1"));
        assertThat(runner)
                .contains("[string]$FaultPlan")
                .contains("faultInjection")
                .contains("coveredByWorkload")
                .contains("suppressedCommandDuplicates")
                .contains("redisCoordinationFailures")
                .contains("permitLimitViolations")
                .contains("traceBatchesAfterRestore")
                .contains("docker compose unpause $service");
        assertThat(injector)
                .contains("ValidateSet(\"postgres\", \"kafka\", \"redis\", \"prometheus\", \"otel-collector\")")
                .contains("ValidateRange(1, 30)")
                .contains("redis-cli FLUSHALL")
                .contains("mirrorReconstructedAt")
                .contains("finally {")
                .contains("docker compose unpause $Service")
                .contains("[System.IO.File]::WriteAllText");

        JsonNode summary = objectMapper.readTree(Files.readString(
                repositoryRoot.resolve("docs/resilience/resilience-summary.json")
        ));
        assertThat(summary.path("schemaVersion").asInt()).isEqualTo(1);
        assertThat(summary.path("scenarioCount").asInt()).isEqualTo(4);
        assertThat(summary.path("passed").asBoolean()).isTrue();
        summary.path("scenarios").forEach(scenario -> {
            assertThat(scenario.path("accepted").asLong()).isEqualTo(scenario.path("succeeded").asLong());
            assertThat(scenario.path("durableFailures").asLong()).isZero();
            assertThat(scenario.path("passed").asBoolean()).isTrue();
        });
        for (String report : List.of(
                "kafka-pause.json", "postgres-pause.json", "redis-loss.json", "telemetry-pause.json"
        )) {
            JsonNode evidence = objectMapper.readTree(Files.readString(
                    repositoryRoot.resolve("docs/resilience/reports/" + report)
            ));
            assertThat(evidence.path("environment").path("machine").asString())
                    .isEqualTo("redacted-local-rancher-host");
            assertThat(evidence.path("passed").asBoolean()).isTrue();
        }
        JsonNode redis = objectMapper.readTree(Files.readString(
                repositoryRoot.resolve("docs/resilience/reports/redis-loss.json")
        ));
        assertThat(redis.path("faultInjection").path("permitLimitViolations").asLong()).isZero();
        assertThat(redis.path("faultInjection").path("redisCoordinationFailures").asLong()).isPositive();
        assertThat(redis.path("faultInjection").path("events").get(1).path("reconstructedKeyCount").asLong())
                .isPositive();

        JsonNode telemetry = objectMapper.readTree(Files.readString(
                repositoryRoot.resolve("docs/resilience/reports/telemetry-pause.json")
        ));
        JsonNode recovered = telemetry.path("faultInjection").path("telemetry").path("after");
        assertThat(recovered.path("prometheusReady").asBoolean()).isTrue();
        assertThat(recovered.path("healthyApplicationTargets").asInt()).isEqualTo(2);
        assertThat(recovered.path("traceBatchesAfterRestore").asInt()).isPositive();
    }

    @Test
    void resilienceMatrixRequiresEveryDependencyScenarioAndCuratesReports() throws Exception {
        JsonNode matrix = objectMapper.readTree(Files.readString(
                repositoryRoot.resolve("load-testing/resilience-matrix.json")
        ));
        assertThat(matrix.path("schemaVersion").asInt()).isEqualTo(1);
        assertThat(matrix.path("name").asString()).isEqualTo("phase-6-resilience-matrix");
        assertThat(matrix.path("scenarios").size()).isEqualTo(4);

        Set<String> ids = new HashSet<>();
        matrix.path("scenarios").forEach(scenario -> {
            String id = scenario.path("id").asString();
            assertThat(ids.add(id)).isTrue();
            assertThat(scenario.path("requiredPass").asBoolean()).isTrue();
            assertThat(repositoryRoot.resolve(
                    "load-testing/faults/" + scenario.path("faultPlan").asString() + ".json"
            )).isRegularFile();
        });
        assertThat(ids).containsExactlyInAnyOrder(
                "postgres-pause", "kafka-pause", "redis-loss", "telemetry-pause"
        );

        String runner = Files.readString(repositoryRoot.resolve("scripts/run-resilience-matrix.ps1"));
        assertThat(runner).contains(
                "run-load-topology.ps1",
                "-SkipBuild -ResetData",
                "requiredFailures",
                "redacted-local-rancher-host",
                "docs/resilience/reports",
                "docs/resilience/resilience-summary.json",
                "[switch]$SummarizeOnly"
        );
    }

    private static Set<String> requiredScenarioIds(JsonNode matrix, boolean required) {
        Set<String> result = new HashSet<>();
        matrix.get("scenarios").forEach(scenario -> {
            if (scenario.get("requiredPass").asBoolean() == required) {
                result.add(scenario.get("id").asString());
            }
        });
        return result;
    }

    private static void assertPorts(JsonNode node, Set<Integer> observed) {
        assertThat(node).isNotNull();
        assertThat(node.size()).isPositive();
        node.forEach(value -> {
            int port = value.asInt();
            assertThat(port).isBetween(1024, 65535);
            assertThat(observed.add(port)).as("port %s must be unique", port).isTrue();
        });
    }

    private static String stripExtension(String fileName) {
        return fileName.substring(0, fileName.length() - ".json".length());
    }

    private static Path findRepositoryRoot() {
        Path candidate = Path.of("").toAbsolutePath();
        while (candidate != null && !Files.isRegularFile(candidate.resolve("mvnw.cmd"))) {
            candidate = candidate.getParent();
        }
        if (candidate == null) throw new IllegalStateException("Could not locate repository root");
        return candidate;
    }
}
