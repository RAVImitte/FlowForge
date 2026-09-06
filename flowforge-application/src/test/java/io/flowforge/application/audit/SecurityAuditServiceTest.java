package io.flowforge.application.audit;

import io.flowforge.domain.tenancy.TenantId;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SecurityAuditServiceTest {
    @Test
    void appliesTheConfiguredOnlineRetentionAndTenantScope() {
        SecurityAuditRepository repository = mock(SecurityAuditRepository.class);
        Instant now = Instant.parse("2026-09-06T12:00:00Z");
        SecurityAuditService service = new SecurityAuditService(
                repository, Clock.fixed(now, ZoneOffset.UTC), Duration.ofDays(90)
        );
        when(repository.findByTenant(eq(TenantId.LOCAL), org.mockito.ArgumentMatchers.any(), anyInt(), anyInt()))
                .thenReturn(List.of());
        when(repository.countByTenant(eq(TenantId.LOCAL), org.mockito.ArgumentMatchers.any())).thenReturn(0L);

        service.history(TenantId.LOCAL, 2, 25);

        ArgumentCaptor<Instant> cutoff = ArgumentCaptor.forClass(Instant.class);
        verify(repository).findByTenant(eq(TenantId.LOCAL), cutoff.capture(), eq(50), eq(25));
        assertThat(cutoff.getValue()).isEqualTo(now.minus(Duration.ofDays(90)));
    }
}
