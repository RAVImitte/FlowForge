package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.AttemptLeaseRecovery;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AttemptLeaseReaperTest {
    @Test
    void reapsExpiredLeasesAndRecordsTheBatchSize() {
        AttemptLeaseRecovery recovery = mock(AttemptLeaseRecovery.class);
        Instant now = Instant.parse("2026-09-05T08:00:00Z");
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        when(recovery.reapExpiredLeases(25, now)).thenReturn(3);
        AttemptLeaseReaper reaper = new AttemptLeaseReaper(
                recovery, Clock.fixed(now, ZoneOffset.UTC), meters, 25
        );

        reaper.reapExpiredLeases();

        verify(recovery).reapExpiredLeases(25, now);
        assertThat(meters.counter("flowforge.leases.reaped").count()).isEqualTo(3.0);
    }
}
