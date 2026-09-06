package io.flowforge.worker.config;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WorkerOperationalMetricsTest {
    @Test
    void exposesWorkerBacklogMetricsWithDescriptionsAndUnits() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), org.mockito.ArgumentMatchers.eq(Double.class))).thenReturn(3.0);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        new WorkerOperationalMetrics(jdbc, registry);

        assertThat(registry.get("flowforge.worker.commands.received").functionCounter().count()).isEqualTo(3.0);
        assertThat(registry.get("flowforge.worker.commands.processing").gauge().value()).isEqualTo(3.0);
        assertThat(registry.get("flowforge.worker.results.oldest.age").gauge().getId().getBaseUnit())
                .isEqualTo("seconds");
        assertThat(registry.getMeters()).allSatisfy(meter -> {
            assertThat(meter.getId().getDescription()).isNotBlank();
            assertThat(meter.getId().getTags()).noneMatch(tag -> tag.getKey().contains("id"));
        });
    }
}
