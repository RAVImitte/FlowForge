package io.flowforge.controlplane.adapter.out.messaging;

import io.flowforge.controlplane.config.OutboxProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxPublisherTest {
    private static final Instant NOW = Instant.parse("2026-09-04T12:00:00Z");
    private static final UUID CLAIM_TOKEN = UUID.randomUUID();

    @Test
    void releasesSendFailuresAndPublishesTheRecoveredMessage() throws Exception {
        OutboxMessageRepository repository = mock(OutboxMessageRepository.class);
        OutboxMessageSender sender = mock(OutboxMessageSender.class);
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        OutboxMessage message = message();
        when(repository.claimBatch(anyInt(), any(String.class), any(Instant.class), any(Duration.class)))
                .thenReturn(List.of(message))
                .thenReturn(List.of(message));
        when(repository.release(any(UUID.class), any(UUID.class), any(Instant.class), any(String.class)))
                .thenReturn(true);
        when(repository.markPublished(message.id(), message.claimToken(), NOW)).thenReturn(true);
        doThrow(new IllegalStateException("broker unavailable"))
                .doNothing()
                .when(sender).send(message, Duration.ofSeconds(5));
        OutboxPublisher publisher = publisher(repository, sender, meters);

        assertThat(publisher.publishAvailable()).isZero();
        assertThat(publisher.publishAvailable()).isEqualTo(1);

        verify(repository).release(message.id(), message.claimToken(), NOW, "broker unavailable");
        verify(repository).markPublished(message.id(), message.claimToken(), NOW);
        assertThat(meters.counter("flowforge.outbox.publish.failures", "topic", message.topic()).count())
                .isEqualTo(1);
        assertThat(meters.counter("flowforge.outbox.published", "topic", message.topic()).count())
                .isEqualTo(1);
    }

    @Test
    void leavesTheLeaseForReplayWhenKafkaAckCannotBePersisted() throws Exception {
        OutboxMessageRepository repository = mock(OutboxMessageRepository.class);
        OutboxMessageSender sender = mock(OutboxMessageSender.class);
        OutboxMessage message = message();
        when(repository.claimBatch(anyInt(), any(String.class), any(Instant.class), any(Duration.class)))
                .thenReturn(List.of(message));
        when(repository.markPublished(message.id(), message.claimToken(), NOW))
                .thenThrow(new IllegalStateException("database unavailable"));
        SimpleMeterRegistry meters = new SimpleMeterRegistry();

        assertThat(publisher(repository, sender, meters).publishAvailable()).isZero();

        verify(sender).send(message, Duration.ofSeconds(5));
        verify(repository, never()).release(
                any(UUID.class), any(UUID.class), any(Instant.class), any(String.class)
        );
        assertThat(meters.counter("flowforge.outbox.acknowledgement.failures", "topic", message.topic()).count())
                .isEqualTo(1);
    }

    @Test
    void leavesTheLeaseWhenTheBrokerOutcomeIsUnknown() throws Exception {
        OutboxMessageRepository repository = mock(OutboxMessageRepository.class);
        OutboxMessageSender sender = mock(OutboxMessageSender.class);
        OutboxMessage message = message();
        when(repository.claimBatch(anyInt(), any(String.class), any(Instant.class), any(Duration.class)))
                .thenReturn(List.of(message));
        doThrow(new TimeoutException("ack timeout")).when(sender).send(message, Duration.ofSeconds(5));

        assertThat(publisher(repository, sender, new SimpleMeterRegistry()).publishAvailable()).isZero();

        verify(repository, never()).release(
                any(UUID.class), any(UUID.class), any(Instant.class), any(String.class)
        );
    }

    private static OutboxPublisher publisher(
            OutboxMessageRepository repository,
            OutboxMessageSender sender,
            SimpleMeterRegistry meters
    ) {
        return new OutboxPublisher(
                repository,
                sender,
                new OutboxProperties(100, Duration.ofSeconds(30), Duration.ofSeconds(5), "test-instance"),
                Clock.fixed(NOW, ZoneOffset.UTC),
                meters
        );
    }

    private static OutboxMessage message() {
        UUID workflowId = UUID.randomUUID();
        return new OutboxMessage(
                UUID.randomUUID(),
                workflowId,
                UUID.randomUUID(),
                "TASK_COMMAND",
                "flowforge.task.commands.v1",
                UUID.randomUUID().toString(),
                "flowforge.task.command",
                1,
                "{}",
                1,
                NOW,
                CLAIM_TOKEN
        );
    }
}
