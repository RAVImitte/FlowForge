package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.DurableTaskQueue;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RetrySchedulingLoopTest {
    @Test
    void releasesDueRetriesAndRecordsTheBatchSize() {
        DurableTaskQueue queue = mock(DurableTaskQueue.class);
        Instant now = Instant.parse("2026-09-04T12:00:00Z");
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        when(queue.releaseDueRetries(25, now)).thenReturn(3);
        RetrySchedulingLoop loop = new RetrySchedulingLoop(
                queue,
                Clock.fixed(now, ZoneOffset.UTC),
                meters,
                25
        );

        loop.releaseDueRetries();

        verify(queue).releaseDueRetries(25, now);
        assertThat(meters.counter("flowforge.retries.released").count()).isEqualTo(3.0);
    }
}
