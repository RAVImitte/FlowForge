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
}
