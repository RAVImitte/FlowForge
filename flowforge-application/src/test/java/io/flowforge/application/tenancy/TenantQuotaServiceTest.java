package io.flowforge.application.tenancy;

import io.flowforge.application.coordination.TokenBucketPolicy;
import io.flowforge.domain.tenancy.TenantId;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TenantQuotaServiceTest {
    private static final Instant NOW = Instant.parse("2026-09-06T12:00:00Z");
    private static final TenantId TENANT = new TenantId("orders-eu");

    @Test
    void returnsTheEffectiveQuotaAndPersistsAnExpectedVersionUpdate() {
        TenantQuotaRepository repository = mock(TenantQuotaRepository.class);
        TenantQuotaProvider provider = mock(TenantQuotaProvider.class);
        TenantQuotaService service = new TenantQuotaService(
                repository, provider, Clock.fixed(NOW, ZoneOffset.UTC)
        );
        TenantQuota inherited = TenantQuota.inherited(TENANT, policy(10));
        TenantQuota configured = new TenantQuota(TENANT, 1, policy(5), true, NOW, NOW);
        when(provider.quotaFor(TENANT)).thenReturn(inherited);
        when(repository.save(TENANT, configured.policy(), 0, NOW)).thenReturn(configured);

        assertThat(service.get(TENANT)).isSameAs(inherited);
        assertThat(service.update(TENANT, configured.policy(), 0)).isSameAs(configured);
        verify(repository).save(TENANT, configured.policy(), 0, NOW);
    }

    @Test
    void rejectsInvalidResourceAndRateLimits() {
        assertThatThrownBy(() -> policy(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxActiveExecutions");
        assertThatThrownBy(() -> new TokenBucketPolicy(2, 0, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static TenantQuotaPolicy policy(int limit) {
        TokenBucketPolicy rate = new TokenBucketPolicy(10, 10, Duration.ofSeconds(1));
        return new TenantQuotaPolicy(limit, 10, 10, 10, rate, rate);
    }
}
