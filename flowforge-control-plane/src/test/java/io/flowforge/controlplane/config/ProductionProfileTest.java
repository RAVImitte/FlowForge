package io.flowforge.controlplane.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ProductionProfileTest {
    @Test
    void defaultsProductionToTheCompleteDistributedTopology() {
        YamlPropertiesFactoryBean loader = new YamlPropertiesFactoryBean();
        loader.setResources(new ClassPathResource("application-production.yml"));
        Properties properties = loader.getObject();

        assertThat(properties).isNotNull();
        assertThat(properties.getProperty("flowforge.kafka.enabled")).isEqualTo("true");
        assertThat(properties.getProperty("flowforge.execution.dispatch-enabled")).isEqualTo("false");
        assertThat(properties.getProperty("flowforge.execution.require-active-dispatcher")).isEqualTo("true");
        assertThat(properties.getProperty("flowforge.outbox.publisher-enabled")).isEqualTo("true");
        assertThat(properties.getProperty("flowforge.outbox.command-dispatch-enabled")).isEqualTo("true");
        assertThat(properties.getProperty("flowforge.results.consumer-enabled")).isEqualTo("true");
        assertThat(properties.getProperty("flowforge.leases.heartbeat-consumer-enabled")).isEqualTo("true");
        assertThat(properties.getProperty("flowforge.leases.reaper-enabled")).isEqualTo("true");

        assertThatCode(() -> new DispatchTopologyValidator(
                true,
                false,
                true,
                true,
                true,
                true,
                true
        ).afterPropertiesSet()).doesNotThrowAnyException();
    }
}
