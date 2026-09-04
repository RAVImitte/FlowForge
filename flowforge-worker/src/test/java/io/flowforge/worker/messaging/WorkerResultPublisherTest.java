package io.flowforge.worker.messaging;

import io.flowforge.worker.config.WorkerExecutionProperties;
import io.flowforge.worker.config.WorkerProperties;
import io.flowforge.worker.persistence.WorkerResultMessage;
import io.flowforge.worker.persistence.WorkerResultOutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WorkerResultPublisherTest {
    private static final Instant NOW = Instant.parse("2026-09-04T12:00:00Z");

    @Test
    void releasesKnownKafkaFailuresForImmediateRetry() {
        WorkerResultOutboxRepository repository = mock(WorkerResultOutboxRepository.class);
        KafkaTemplate<String, String> kafka = kafkaTemplate();
        WorkerResultMessage message = message();
        when(repository.claimBatch(anyInt(), any(String.class), any(Instant.class), any(Duration.class)))
                .thenReturn(List.of(message));
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        SimpleMeterRegistry meters = new SimpleMeterRegistry();

        assertThat(publisher(repository, kafka, meters, Duration.ofSeconds(1)).publishAvailable()).isZero();

        verify(repository).release(message.id(), message.claimToken(), NOW, "broker unavailable");
        assertThat(meters.counter("flowforge.worker.results.publish.failures").count()).isEqualTo(1);
    }

    @Test
    void retainsLeaseWhenKafkaOutcomeIsUnknown() {
        WorkerResultOutboxRepository repository = mock(WorkerResultOutboxRepository.class);
        KafkaTemplate<String, String> kafka = kafkaTemplate();
        WorkerResultMessage message = message();
        when(repository.claimBatch(anyInt(), any(String.class), any(Instant.class), any(Duration.class)))
                .thenReturn(List.of(message));
        when(kafka.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<>());
        SimpleMeterRegistry meters = new SimpleMeterRegistry();

        assertThat(publisher(repository, kafka, meters, Duration.ofMillis(1)).publishAvailable()).isZero();

        verify(repository, never()).release(any(), any(), any(), any());
        verify(repository, never()).markPublished(any(), any(), any());
        assertThat(meters.counter("flowforge.worker.results.unknown.outcomes").count()).isEqualTo(1);
    }

    @Test
    void retainsLeaseWhenKafkaAckCannotBePersisted() {
        WorkerResultOutboxRepository repository = mock(WorkerResultOutboxRepository.class);
        KafkaTemplate<String, String> kafka = kafkaTemplate();
        WorkerResultMessage message = message();
        when(repository.claimBatch(anyInt(), any(String.class), any(Instant.class), any(Duration.class)))
                .thenReturn(List.of(message));
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
        when(repository.markPublished(message.id(), message.claimToken(), NOW))
                .thenThrow(new IllegalStateException("database unavailable"));
        SimpleMeterRegistry meters = new SimpleMeterRegistry();

        assertThat(publisher(repository, kafka, meters, Duration.ofSeconds(1)).publishAvailable()).isZero();

        verify(repository, never()).release(any(), any(), any(), any());
        assertThat(meters.counter("flowforge.worker.results.acknowledgement.failures").count()).isEqualTo(1);
    }

    private static WorkerResultPublisher publisher(
            WorkerResultOutboxRepository repository,
            KafkaTemplate<String, String> kafka,
            SimpleMeterRegistry meters,
            Duration publishTimeout
    ) {
        return new WorkerResultPublisher(
                repository,
                kafka,
                new WorkerProperties("worker-test", "flowforge-workers-v1", Duration.ofSeconds(1)),
                new WorkerExecutionProperties(1, 100, Duration.ofSeconds(30), publishTimeout),
                Clock.fixed(NOW, ZoneOffset.UTC),
                meters
        );
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, String> kafkaTemplate() {
        return mock(KafkaTemplate.class);
    }

    private static WorkerResultMessage message() {
        return new WorkerResultMessage(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "flowforge.task.results.v1",
                UUID.randomUUID().toString(),
                "flowforge.task.result",
                1,
                "{}",
                1,
                UUID.randomUUID()
        );
    }
}
