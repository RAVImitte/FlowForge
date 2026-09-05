package io.flowforge.kafka;

import io.flowforge.messaging.FlowForgeTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaConsumerRecoveryFactoryTest {
    private static final Clock CLOCK = Clock.fixed(
            Instant.parse("2026-09-05T12:00:00Z"), ZoneOffset.UTC
    );

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void preservesRawRecordAndAddsStableRecoveryMetadata() {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
        var publisher = KafkaConsumerRecoveryFactory.publisher(kafka, CLOCK, Duration.ofSeconds(2));
        ConsumerRecord<String, String> source = new ConsumerRecord<>(
                FlowForgeTopics.TASK_RESULTS_V1, 2, 41, "workflow", "{broken"
        );
        source.headers().add("flowforge-correlation-id", "workflow".getBytes(StandardCharsets.UTF_8));

        publisher.accept(source, new IllegalArgumentException("invalid payload"));

        ArgumentCaptor<ProducerRecord<String, String>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafka).send(sent.capture());
        assertThat(sent.getValue().topic()).isEqualTo(FlowForgeTopics.TASK_RESULTS_DLQ_V1);
        assertThat(sent.getValue().partition()).isEqualTo(2);
        assertThat(sent.getValue().key()).isEqualTo("workflow");
        assertThat(sent.getValue().value()).isEqualTo("{broken");
        assertThat(header(sent.getValue(), KafkaConsumerRecoveryFactory.DLQ_SCHEMA_VERSION_HEADER)).isEqualTo("1");
        assertThat(header(sent.getValue(), KafkaConsumerRecoveryFactory.DLQ_RECORD_ID_HEADER))
                .isEqualTo(FlowForgeTopics.TASK_RESULTS_V1 + ":2:41");
        assertThat(header(sent.getValue(), KafkaConsumerRecoveryFactory.DLQ_FAILURE_CLASS_HEADER))
                .isEqualTo(IllegalArgumentException.class.getName());
        assertThat(header(sent.getValue(), "flowforge-correlation-id")).isEqualTo("workflow");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void treatsMissingBrokerAcknowledgementAsRecoveryFailure() {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, String>> rejected = new CompletableFuture<>();
        rejected.completeExceptionally(new IllegalStateException("broker unavailable"));
        when(kafka.send(any(ProducerRecord.class))).thenReturn(rejected);
        var publisher = KafkaConsumerRecoveryFactory.publisher(kafka, CLOCK, Duration.ofSeconds(2));
        ConsumerRecord<String, String> source = new ConsumerRecord<>(
                FlowForgeTopics.TASK_HEARTBEATS_V1, 0, 7, "task", "invalid"
        );

        assertThatThrownBy(() -> publisher.accept(source, new IllegalArgumentException("invalid payload")))
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("Dead-letter");
    }

    @Test
    void rejectsUnboundedOrInvalidRecoverySettings() {
        assertThatThrownBy(() -> KafkaConsumerRecoveryFactory.backOff(
                -1, Duration.ofMillis(10), 2.0, Duration.ofSeconds(1)
        )).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaConsumerRecoveryFactory.backOff(
                2, Duration.ZERO, 2.0, Duration.ofSeconds(1)
        )).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> KafkaConsumerRecoveryFactory.backOff(
                2, Duration.ofSeconds(2), 2.0, Duration.ofSeconds(1)
        )).isInstanceOf(IllegalArgumentException.class);
    }

    private static String header(ProducerRecord<?, ?> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
