package io.flowforge.kafka;

import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaProducerWarmupTest {
    @Test
    void resolvesEveryOutboundTopicBeforeStartupCompletes() {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.partitionsFor("commands")).thenReturn(List.of(mock(PartitionInfo.class)));
        when(kafka.partitionsFor("events")).thenReturn(List.of(mock(PartitionInfo.class)));

        new KafkaProducerWarmup(kafka).warm(List.of("commands", "events"));

        verify(kafka).partitionsFor("commands");
        verify(kafka).partitionsFor("events");
    }

    @Test
    void failsStartupWhenMetadataIsUnavailable() {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.partitionsFor("commands")).thenReturn(List.of());

        assertThatThrownBy(() -> new KafkaProducerWarmup(kafka).warm(List.of("commands")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("commands");
    }
}
