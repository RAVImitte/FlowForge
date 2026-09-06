package io.flowforge.observability;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class ObservabilityAssetsTest {
    private static final Set<String> REQUIRED_METRICS = Set.of(
            "flowforge_workflows_started_total",
            "flowforge_workflows_completed_total",
            "flowforge_executions_active",
            "flowforge_tasks_ready",
            "flowforge_tasks_ready_oldest_age_seconds",
            "flowforge_outbox_pending",
            "flowforge_outbox_oldest_age_seconds",
            "flowforge_worker_results_pending",
            "flowforge_worker_results_oldest_age_seconds",
            "flowforge_retries_scheduled_total",
            "flowforge_tasks_dead_lettered_total",
            "flowforge_schedules_pending",
            "flowforge_concurrency_permits_active"
    );
    private static final Set<String> ALERT_SOURCE_METRICS = Set.of(
            "flowforge_tasks_ready_oldest_age_seconds",
            "flowforge_executions_active_oldest_age_seconds",
            "flowforge_outbox_pending",
            "flowforge_outbox_oldest_age_seconds",
            "flowforge_worker_results_pending",
            "flowforge_worker_results_oldest_age_seconds",
            "flowforge_schedules_pending",
            "flowforge_schedules_pending_current_oldest_age_seconds",
            "flowforge_retries_exhausted_total",
            "flowforge_timeouts_tasks_total",
            "flowforge_tasks_dead_lettered_total",
            "flowforge_kafka_dlq_published_total",
            "flowforge_concurrency_permits_active",
            "flowforge_executions_active",
            "flowforge_tasks_running",
            "flowforge_admission_throttled_total",
            "flowforge_admission_saturated_total",
            "flowforge_admission_rejected_total",
            "flowforge_coordination_redis_failures_total"
    );

    @Test
    void dashboardIsVersionedAndReferencesOperationalMetricsWithoutEntityLabels() throws IOException {
        Path root = repositoryRoot();
        JsonNode dashboard = new ObjectMapper().readTree(Files.readString(
                root.resolve("observability/grafana/dashboards/flowforge-overview.json")));

        assertThat(dashboard.path("uid").asString()).isEqualTo("flowforge-overview");
        assertThat(dashboard.path("version").asInt()).isPositive();
        assertThat(dashboard.path("panels").size()).isGreaterThanOrEqualTo(10);

        Set<String> expressions = new HashSet<>();
        dashboard.path("panels").forEach(panel -> panel.path("targets").forEach(target -> {
            if (!target.path("expr").isMissingNode()) expressions.add(target.path("expr").asString());
        }));
        String joined = String.join("\n", expressions);

        assertThat(REQUIRED_METRICS).allSatisfy(metric -> assertThat(joined).contains(metric));
        assertThat(joined).doesNotContain(
                "workflow_execution_id", "task_execution_id", "attempt_id", "event_id", "fencing_token"
        );
    }

    @Test
    void prometheusScrapesBothApplicationsAndGrafanaUsesTheProvisionedDatasource() throws IOException {
        Path root = repositoryRoot();
        String prometheus = Files.readString(root.resolve("observability/prometheus/prometheus.yml"));
        String datasource = Files.readString(
                root.resolve("observability/grafana/provisioning/datasources/prometheus.yml"));
        String provider = Files.readString(
                root.resolve("observability/grafana/provisioning/dashboards/flowforge.yml"));

        assertThat(prometheus)
                .contains("metrics_path: /actuator/prometheus")
                .contains("host.docker.internal:8080")
                .contains("host.docker.internal:8081")
                .contains("/etc/prometheus/rules/*.rules.yml");
        assertThat(datasource)
                .contains("uid: flowforge-prometheus")
                .contains("url: http://prometheus:9090");
        assertThat(provider).contains("path: /var/lib/grafana/dashboards");
    }

    @Test
    void alertRulesCoverSlosAndOperationalSymptomsWithActionableRunbooks() throws IOException {
        Path root = repositoryRoot();
        String recordingRules = Files.readString(root.resolve(
                "observability/prometheus/rules/flowforge-slo-recording.rules.yml"));
        String alertRules = Files.readString(root.resolve(
                "observability/prometheus/rules/flowforge-alerts.rules.yml"));
        String compose = Files.readString(root.resolve("compose.yaml"));

        assertThat(recordingRules).contains(
                "flowforge:sli_api_error:ratio_rate5m",
                "flowforge:sli_api_error:ratio_rate3d",
                "flowforge:sli_workflow_start_slow:ratio_rate5m",
                "flowforge:sli_workflow_start_slow:ratio_rate3d",
                "flowforge:sli_task_queue_oldest:seconds",
                "flowforge:sli_terminal_oldest_active:seconds",
                "status=\"202\"",
                "flowforge_executions_active_oldest_age_seconds"
        );
        assertThat(alertRules).contains(
                "FlowForgeApiAvailabilityFastBurn",
                "FlowForgeWorkflowStartLatencyFastBurn",
                "FlowForgeTaskQueueLatencyBreach",
                "FlowForgeTerminalCompletionBreach",
                "FlowForgeControlPlaneOutboxStalled",
                "FlowForgeWorkerResultOutboxStalled",
                "FlowForgeRetryExhaustionSpike",
                "FlowForgeTaskTimeoutSpike",
                "FlowForgeCoordinationPermitLeak",
                "FlowForgeAdmissionSaturation",
                "FlowForgeDeadLetterTraffic",
                "FlowForgeServiceTargetDown"
        );
        int alertCount = occurrences(alertRules, "      - alert:");
        assertThat(alertCount).isGreaterThanOrEqualTo(12);
        assertThat(occurrences(alertRules, "          action:")).isEqualTo(alertCount);
        assertThat(occurrences(alertRules, "          runbook_url:")).isEqualTo(alertCount);
        assertThat(recordingRules + alertRules).doesNotContain(
                "workflow_execution_id", "task_execution_id", "attempt_id", "event_id", "fencing_token"
        );
        assertThat(matches(recordingRules + alertRules, "\\bflowforge_[a-z0-9_]+\\b"))
                .isEqualTo(ALERT_SOURCE_METRICS);
        assertThat(matches(alertRules, "\\bflowforge:[a-z0-9_:]+\\b"))
                .isSubsetOf(matches(recordingRules, "(?m)^\\s*- record: (flowforge:[a-z0-9_:]+)$"));
        assertThat(compose).contains(
                "./observability/prometheus/rules:/etc/prometheus/rules:ro"
        );

        Matcher links = Pattern.compile("runbook_url: .*/blob/main/([^#\\s]+)(?:#[^\\s]+)?").matcher(alertRules);
        int linkCount = 0;
        while (links.find()) {
            assertThat(root.resolve(links.group(1))).exists().isRegularFile();
            linkCount++;
        }
        assertThat(linkCount).isEqualTo(alertCount);
    }

    @Test
    void phaseSixOperationsAndArchitectureDocumentTelemetryBoundaries() throws IOException {
        Path root = repositoryRoot();
        String runbook = Files.readString(root.resolve(
                "docs/operations/phase-6-observability-capacity-resilience-runbook.md"));
        String adr = Files.readString(root.resolve(
                "docs/adr/006-observability-scalability-and-resilience.md"));

        assertThat(runbook).contains(
                "PostgreSQL is the durable correctness boundary",
                "host.docker.internal",
                "flowforge-overview",
                "run-resilience-matrix.ps1",
                "run-capacity-matrix.ps1",
                "verify-observability.ps1",
                "phase-6-slo-alerting-runbook.md",
                "phase-4-reliability-runbook.md",
                "phase-5-scheduling-coordination-runbook.md",
                "Redis pause plus state flush",
                "simultaneous Prometheus/collector pause"
        );
        assertThat(adr).contains(
                "Accepted",
                "Fail-open telemetry",
                "traceparent",
                "tracestate",
                "baggage",
                "NaN",
                "Workflow, execution, task, attempt, event, correlation, trace, idempotency",
                "never part of a database transaction, Kafka acknowledgement, lease, fencing, idempotency",
                "Phase 6 operations runbook"
        );
        assertThat(adr).doesNotContain("workflow_execution_id=", "attempt_id=", "trace_id=");
    }

    private static int occurrences(String text, String token) {
        return (text.length() - text.replace(token, "").length()) / token.length();
    }

    private static Set<String> matches(String text, String expression) {
        Set<String> matches = new HashSet<>();
        Matcher matcher = Pattern.compile(expression).matcher(text);
        while (matcher.find()) matches.add(matcher.group(matcher.groupCount() == 0 ? 0 : 1));
        return matches;
    }

    private static Path repositoryRoot() {
        Path current = Path.of("").toAbsolutePath();
        if (Files.isDirectory(current.resolve("observability"))) return current;
        Path parent = current.getParent();
        if (parent != null && Files.isDirectory(parent.resolve("observability"))) return parent;
        throw new IllegalStateException("Could not locate the FlowForge repository root from " + current);
    }
}
