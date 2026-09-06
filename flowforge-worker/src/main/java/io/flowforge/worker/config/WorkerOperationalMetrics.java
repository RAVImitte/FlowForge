package io.flowforge.worker.config;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public final class WorkerOperationalMetrics {
    private static final Logger log = LoggerFactory.getLogger(WorkerOperationalMetrics.class);

    private final JdbcTemplate jdbc;

    public WorkerOperationalMetrics(JdbcTemplate jdbc, MeterRegistry registry) {
        this.jdbc = jdbc;

        FunctionCounter.builder("flowforge.worker.commands.received", this,
                        source -> source.query("SELECT count(*) FROM worker_command_inbox"))
                .description("Durable task commands accepted by this worker database")
                .register(registry);
        Gauge.builder("flowforge.worker.commands.processing", this,
                        source -> source.query("SELECT count(*) FROM worker_command_inbox WHERE status = 'RECEIVED'"))
                .description("Task commands that have not completed processing")
                .register(registry);
        Gauge.builder("flowforge.worker.results.oldest.age", this,
                        source -> source.query("""
                                SELECT COALESCE(EXTRACT(EPOCH FROM (clock_timestamp() - min(available_at))), 0)
                                  FROM worker_result_outbox
                                 WHERE status = 'PENDING'
                                """))
                .description("Age in seconds of the oldest publishable worker result")
                .baseUnit("seconds")
                .register(registry);
    }

    private double query(String sql) {
        try {
            return Objects.requireNonNullElse(jdbc.queryForObject(sql, Double.class), 0.0);
        } catch (RuntimeException exception) {
            log.debug("Operational metric query failed", exception);
            return Double.NaN;
        }
    }
}
