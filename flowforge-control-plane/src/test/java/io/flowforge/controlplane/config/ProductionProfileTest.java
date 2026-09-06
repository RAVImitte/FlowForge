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
        assertThat(properties.getProperty("flowforge.security.enabled"))
                .isEqualTo("${FLOWFORGE_SECURITY_ENABLED:true}");
        assertThat(properties.getProperty("logging.structured.format.console"))
                .isEqualTo("${FLOWFORGE_LOG_FORMAT:ecs}");

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

    @Test
    void keepsLocalSecurityOptInAndRequiresAnIssuerWhenEnabled() {
        YamlPropertiesFactoryBean loader = new YamlPropertiesFactoryBean();
        loader.setResources(new ClassPathResource("application.yml"));
        Properties properties = loader.getObject();

        assertThat(properties).isNotNull();
        assertThat(properties.getProperty("flowforge.security.enabled"))
                .isEqualTo("${FLOWFORGE_SECURITY_ENABLED:false}");
        assertThat(properties.getProperty("flowforge.security.issuer-uri"))
                .isEqualTo("${FLOWFORGE_OIDC_ISSUER_URI:}");
        assertThat(properties.getProperty("flowforge.security.jwk-set-uri"))
                .isEqualTo("${FLOWFORGE_OIDC_JWK_SET_URI:}");
        assertThat(properties.getProperty("flowforge.security.roles-claim"))
                .isEqualTo("${FLOWFORGE_SECURITY_ROLES_CLAIM:roles}");
        assertThat(properties.getProperty("flowforge.security.tenant-id-claim"))
                .isEqualTo("${FLOWFORGE_SECURITY_TENANT_ID_CLAIM:tenant_id}");
        assertThat(properties.getProperty("flowforge.security.local-tenant-id"))
                .isEqualTo("${FLOWFORGE_LOCAL_TENANT_ID:local}");

        FlowForgeSecurityProperties security = new FlowForgeSecurityProperties();
        security.setEnabled(true);
        assertThatCode(security::requiredIssuerUri)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("FLOWFORGE_OIDC_ISSUER_URI");

        security.setIssuerUri("https://identity.example.test/realms/flowforge");
        assertThatCode(security::requiredJwkSetUri)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("FLOWFORGE_OIDC_JWK_SET_URI");

        security.setJwkSetUri("https://identity.example.test/realms/flowforge/jwks");
        assertThatCode(() -> new SecurityConfiguration().flowForgeJwtDecoder(security))
                .doesNotThrowAnyException();
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
