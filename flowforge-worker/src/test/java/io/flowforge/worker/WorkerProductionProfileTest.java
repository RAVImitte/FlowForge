package io.flowforge.worker;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerProductionProfileTest {
    @Test
    void enablesEcsConsoleLoggingByDefaultInProduction() {
        YamlPropertiesFactoryBean loader = new YamlPropertiesFactoryBean();
        loader.setResources(new ClassPathResource("application-production.yml"));
        Properties properties = loader.getObject();

        assertThat(properties).isNotNull();
        assertThat(properties.getProperty("logging.structured.format.console"))
                .isEqualTo("${FLOWFORGE_LOG_FORMAT:ecs}");
    }

    @Test
    void keepsTraceExportOptInAndBounded() {
        YamlPropertiesFactoryBean loader = new YamlPropertiesFactoryBean();
        loader.setResources(new ClassPathResource("application.yml"));
        Properties properties = loader.getObject();

        assertThat(properties).isNotNull();
        assertThat(properties.getProperty("management.tracing.export.otlp.enabled"))
                .isEqualTo("${FLOWFORGE_OTLP_ENABLED:false}");
        assertThat(properties.getProperty("management.tracing.sampling.probability"))
                .isEqualTo("${FLOWFORGE_TRACING_SAMPLING_PROBABILITY:0.1}");
        assertThat(properties.getProperty("management.opentelemetry.tracing.limits.max-attributes"))
                .isEqualTo("${FLOWFORGE_TRACE_MAX_ATTRIBUTES:64}");
        assertThat(properties.getProperty("management.otlp.metrics.export.enabled")).isEqualTo("false");
        assertThat(properties.getProperty("management.prometheus.metrics.export.enabled"))
                .isEqualTo("${FLOWFORGE_PROMETHEUS_ENABLED:true}");
        assertThat(properties.getProperty("management.endpoints.web.exposure.include"))
                .isEqualTo("health,info,metrics,prometheus");
        assertThat(properties.getProperty("management.metrics.tags.application"))
                .isEqualTo("${spring.application.name}");
        assertThat(properties.getProperty("management.metrics.tags.environment"))
                .isEqualTo("${FLOWFORGE_ENVIRONMENT:local}");
    }
}
