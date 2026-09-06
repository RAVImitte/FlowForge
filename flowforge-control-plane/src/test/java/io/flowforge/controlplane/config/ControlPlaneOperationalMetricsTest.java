package io.flowforge.controlplane.config;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ControlPlaneOperationalMetricsTest {
    @Test
    void exposesDurableQueueAndLifecycleMetricsWithoutEntityTags() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), org.mockito.ArgumentMatchers.eq(Double.class))).thenReturn(7.0);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        new ControlPlaneOperationalMetrics(jdbc, registry);

        assertThat(registry.get("flowforge.executions.active").gauge().value()).isEqualTo(7.0);
        assertThat(registry.get("flowforge.executions.active.oldest.age").gauge().getId().getBaseUnit())
                .isEqualTo("seconds");
        assertThat(registry.get("flowforge.tasks.ready").gauge().value()).isEqualTo(7.0);
        assertThat(registry.get("flowforge.tasks.ready.oldest.age").gauge().getId().getBaseUnit())
                .isEqualTo("seconds");
        assertThat(registry.get("flowforge.workflows.started").functionCounter().count()).isEqualTo(7.0);
        assertThat(registry.get("flowforge.workflows.completed").tag("outcome", "succeeded")
                .functionCounter().count()).isEqualTo(7.0);
        assertThat(registry.getMeters()).allSatisfy(meter ->
                assertThat(meter.getId().getTags()).noneMatch(tag -> tag.getKey().contains("id")));
    }

    @Test
    void returnsNanInsteadOfBreakingScrapesWhenPostgresIsUnavailable() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), org.mockito.ArgumentMatchers.eq(Double.class)))
                .thenThrow(new IllegalStateException("database unavailable"));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        new ControlPlaneOperationalMetrics(jdbc, registry);

        assertThat(registry.get("flowforge.executions.active").gauge().value()).isNaN();
        assertThat(registry.get("flowforge.workflows.started").functionCounter().count()).isNaN();
    }
}
