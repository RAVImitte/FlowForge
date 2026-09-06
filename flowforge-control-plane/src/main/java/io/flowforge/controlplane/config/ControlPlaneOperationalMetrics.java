package io.flowforge.controlplane.config;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public final class ControlPlaneOperationalMetrics {
    private static final Logger log = LoggerFactory.getLogger(ControlPlaneOperationalMetrics.class);

    private final JdbcTemplate jdbc;

    public ControlPlaneOperationalMetrics(JdbcTemplate jdbc, MeterRegistry registry) {
        this.jdbc = jdbc;

        counter(registry, "flowforge.workflows.started", "Durable workflow executions started",
                "SELECT count(*) FROM execution_event WHERE event_type = 'WORKFLOW_STARTED'");
        terminalCounter(registry, "SUCCEEDED");
        terminalCounter(registry, "FAILED");
        terminalCounter(registry, "CANCELLED");

        gauge(registry, "flowforge.executions.active", "Active workflow executions",
                "SELECT count(*) FROM workflow_execution WHERE status IN ('PENDING', 'RUNNING', 'CANCELLING')");
        gauge(registry, "flowforge.executions.active.oldest.age", "Age in seconds of the oldest active workflow execution",
                ageSql("workflow_execution", "status IN ('PENDING', 'RUNNING', 'CANCELLING')", "created_at"), "seconds");
        gauge(registry, "flowforge.tasks.ready", "Tasks waiting in the durable ready queue",
                "SELECT count(*) FROM task_execution WHERE status = 'READY'");
        gauge(registry, "flowforge.tasks.running", "Tasks with an active attempt",
                "SELECT count(*) FROM task_execution WHERE status = 'RUNNING'");
        gauge(registry, "flowforge.tasks.ready.oldest.age", "Age in seconds of the oldest ready task",
                ageSql("task_execution", "status = 'READY'", "created_at"), "seconds");
        gauge(registry, "flowforge.outbox.oldest.age", "Age in seconds of the oldest publishable control-plane outbox record",
                ageSql("control_plane_outbox", "status = 'PENDING'", "available_at"), "seconds");
        gauge(registry, "flowforge.schedules.pending", "Pending durable schedule triggers",
                "SELECT count(*) FROM workflow_schedule_trigger WHERE status = 'PENDING'");
        gauge(registry, "flowforge.schedules.pending.current.oldest.age", "Current age in seconds of the oldest pending schedule trigger",
                ageSql("workflow_schedule_trigger", "status = 'PENDING'", "created_at"), "seconds");
        gauge(registry, "flowforge.concurrency.permits.active", "Active PostgreSQL coordination permits",
                "SELECT count(*) FROM coordination_permit WHERE status = 'ACTIVE' AND expires_at > clock_timestamp()");
    }

    private void terminalCounter(MeterRegistry registry, String outcome) {
        FunctionCounter.builder("flowforge.workflows.completed", this,
                        source -> source.query("SELECT count(*) FROM workflow_execution WHERE status = '" + outcome + "'"))
                .description("Durable workflow executions reaching a terminal state")
                .tag("outcome", outcome.toLowerCase())
                .register(registry);
    }

    private void counter(MeterRegistry registry, String name, String description, String sql) {
        FunctionCounter.builder(name, this, source -> source.query(sql))
                .description(description)
                .register(registry);
    }

    private void gauge(MeterRegistry registry, String name, String description, String sql) {
        gauge(registry, name, description, sql, null);
    }

    private void gauge(MeterRegistry registry, String name, String description, String sql, String baseUnit) {
        Gauge.Builder<ControlPlaneOperationalMetrics> builder = Gauge.builder(name, this, source -> source.query(sql))
                .description(description);
        if (baseUnit != null) builder.baseUnit(baseUnit);
        builder.register(registry);
    }

    private double query(String sql) {
        try {
            return Objects.requireNonNullElse(jdbc.queryForObject(sql, Double.class), 0.0);
        } catch (RuntimeException exception) {
            log.debug("Operational metric query failed", exception);
            return Double.NaN;
        }
    }

    private static String ageSql(String table, String predicate, String timestampColumn) {
        return "SELECT COALESCE(EXTRACT(EPOCH FROM (clock_timestamp() - min(" + timestampColumn + "))), 0) "
                + "FROM " + table + " WHERE " + predicate;
    }
}
