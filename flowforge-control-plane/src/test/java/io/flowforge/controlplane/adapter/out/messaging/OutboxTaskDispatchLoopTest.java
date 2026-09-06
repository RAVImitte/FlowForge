package io.flowforge.controlplane.adapter.out.messaging;

import io.flowforge.application.execution.DurableTaskQueue;
import io.flowforge.controlplane.config.OutboxProperties;
import io.flowforge.domain.tenancy.TenantId;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

class OutboxTaskDispatchLoopTest {
    private static final Instant NOW = Instant.parse("2026-09-04T12:00:00Z");
    private static final OutboxProperties PROPERTIES = new OutboxProperties(
            100,
            8,
            Duration.ofSeconds(30),
            Duration.ofSeconds(5),
            "test-instance"
    );

    @Test
    void enqueuesReadyTasksUsingTheConfiguredBatch() {
        DurableTaskQueue queue = mock(DurableTaskQueue.class);
        OutboxTaskDispatchLoop loop = new OutboxTaskDispatchLoop(
                queue,
                PROPERTIES,
                Clock.fixed(NOW, ZoneOffset.UTC),
                false
        );
        when(queue.readyTenants(NOW, 100)).thenReturn(java.util.List.of(TenantId.LOCAL));

        loop.dispatchReadyTasks();

        verify(queue).enqueueReadyTasks(TenantId.LOCAL, 100, NOW);
    }

    @Test
    void rejectsCompetingInProcessAndOutboxDispatchers() {
        assertThatThrownBy(() -> new OutboxTaskDispatchLoop(
                mock(DurableTaskQueue.class),
                PROPERTIES,
                Clock.fixed(NOW, ZoneOffset.UTC),
                true
        )).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot be enabled together");
    }
}
