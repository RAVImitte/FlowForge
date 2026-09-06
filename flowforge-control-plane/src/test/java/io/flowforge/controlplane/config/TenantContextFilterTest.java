package io.flowforge.controlplane.config;

import io.flowforge.observability.LogFields;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import tools.jackson.databind.json.JsonMapper;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class TenantContextFilterTest {
    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    void suppliesAndCleansUpTheConfiguredLocalTenantWhenSecurityIsDisabled() throws Exception {
        FlowForgeSecurityProperties properties = new FlowForgeSecurityProperties();
        properties.setEnabled(false);
        properties.setLocalTenantId("developer-one");
        TenantContextFilter filter = new TenantContextFilter(properties, JsonMapper.builder().build());
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/v1/workflows");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<String> observedTenant = new AtomicReference<>();

        filter.doFilter(request, response, (filteredRequest, ignored) -> {
            observedTenant.set(TenantContextFilter.requireTenant((MockHttpServletRequest) filteredRequest).value());
            assertThat(MDC.get(LogFields.TENANT_ID)).isEqualTo("developer-one");
        });

        assertThat(observedTenant).hasValue("developer-one");
        assertThat(request.getAttribute(TenantContextFilter.class.getName() + ".tenantId")).isNull();
        assertThat(MDC.get(LogFields.TENANT_ID)).isNull();
    }
}
