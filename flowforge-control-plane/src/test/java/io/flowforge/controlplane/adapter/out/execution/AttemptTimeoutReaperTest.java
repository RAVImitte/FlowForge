package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.AttemptTimeoutRecovery;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AttemptTimeoutReaperTest {
    @Test
    void reapsDueAttemptsAndRecordsTheBatchSize() {
        AttemptTimeoutRecovery recovery = mock(AttemptTimeoutRecovery.class);
        Instant now = Instant.parse("2026-09-04T12:00:00Z");
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        when(recovery.reapTimedOutAttempts(25, now)).thenReturn(3);
        AttemptTimeoutReaper reaper = new AttemptTimeoutReaper(
                recovery,
                Clock.fixed(now, ZoneOffset.UTC),
                meters,
                25
        );

        reaper.reapTimedOutAttempts();

        verify(recovery).reapTimedOutAttempts(25, now);
        assertThat(meters.counter("flowforge.timeouts.reaped").count()).isEqualTo(3.0);
    }
}
