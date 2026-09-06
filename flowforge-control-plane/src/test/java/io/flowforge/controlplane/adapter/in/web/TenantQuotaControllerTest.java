package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.coordination.TokenBucketPolicy;
import io.flowforge.application.tenancy.TenantQuota;
import io.flowforge.application.tenancy.TenantQuotaPolicy;
import io.flowforge.application.tenancy.TenantQuotaProvider;
import io.flowforge.application.tenancy.TenantQuotaRepository;
import io.flowforge.application.tenancy.TenantQuotaService;
import io.flowforge.controlplane.config.FlowForgeSecurityProperties;
import io.flowforge.controlplane.config.TenantContextFilter;
import io.flowforge.domain.tenancy.TenantId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class TenantQuotaControllerTest {
    private static final Instant NOW = Instant.parse("2026-09-06T12:00:00Z");
    private static final String BODY = """
            {
              "maxActiveExecutions": 5,
              "maxRunningTasks": 10,
              "maxPendingScheduleFires": 20,
              "maxReadyTasks": 30,
              "scheduleRateLimit": {"capacity": 4, "refillTokens": 2, "refillPeriodMillis": 1000},
              "dispatchRateLimit": {"capacity": 8, "refillTokens": 4, "refillPeriodMillis": 1000}
            }
            """;

    private TenantQuotaRepository repository;
    private TenantQuotaProvider provider;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        repository = mock(TenantQuotaRepository.class);
        provider = mock(TenantQuotaProvider.class);
        TenantQuotaService service = new TenantQuotaService(
                repository, provider, Clock.fixed(NOW, ZoneOffset.UTC)
        );
        FlowForgeSecurityProperties properties = new FlowForgeSecurityProperties();
        properties.setEnabled(false);
        mvc = MockMvcBuilders.standaloneSetup(new TenantQuotaController(service))
                .setControllerAdvice(new ApiExceptionHandler())
                .addFilters(new TenantContextFilter(properties, JsonMapper.builder().build()))
                .build();
    }

    @Test
    void returnsInheritedDefaultsWithVersionZero() throws Exception {
        when(provider.quotaFor(TenantId.LOCAL))
                .thenReturn(TenantQuota.inherited(TenantId.LOCAL, policy(10)));

        mvc.perform(get("/api/v1/tenant/quota"))
                .andExpect(status().isOk())
                .andExpect(header().string("ETag", "\"0\""))
                .andExpect(jsonPath("$.configured").value(false))
                .andExpect(jsonPath("$.maxActiveExecutions").value(10));
    }

    @Test
    void createsAnOverrideUsingOptimisticVersionZero() throws Exception {
        TenantQuotaPolicy policy = new TenantQuotaPolicy(
                5, 10, 20, 30,
                new TokenBucketPolicy(4, 2, Duration.ofSeconds(1)),
                new TokenBucketPolicy(8, 4, Duration.ofSeconds(1))
        );
        when(repository.save(TenantId.LOCAL, policy, 0, NOW))
                .thenReturn(new TenantQuota(TenantId.LOCAL, 1, policy, true, NOW, NOW));

        mvc.perform(put("/api/v1/tenant/quota")
                        .header("If-Match", "\"0\"")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(BODY))
                .andExpect(status().isOk())
                .andExpect(header().string("ETag", "\"1\""))
                .andExpect(jsonPath("$.configured").value(true))
                .andExpect(jsonPath("$.dispatchRateLimit.capacity").value(8));
    }

    @Test
    void requiresAnExpectedVersionForUpdates() throws Exception {
        mvc.perform(put("/api/v1/tenant/quota")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(BODY))
                .andExpect(status().isPreconditionRequired())
                .andExpect(jsonPath("$.code").value("IF_MATCH_REQUIRED"));
    }

    private static TenantQuotaPolicy policy(int limit) {
        TokenBucketPolicy rate = new TokenBucketPolicy(10, 10, Duration.ofSeconds(1));
        return new TenantQuotaPolicy(limit, 10, 10, 10, rate, rate);
    }
}
