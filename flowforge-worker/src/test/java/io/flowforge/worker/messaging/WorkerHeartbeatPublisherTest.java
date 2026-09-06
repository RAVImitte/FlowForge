package io.flowforge.worker.messaging;

import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.FlowForgeHeaders;
import io.flowforge.messaging.TaskCommandV1;
import io.flowforge.worker.config.WorkerProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WorkerHeartbeatPublisherTest {
    @Test
    @SuppressWarnings("unchecked")
    void schedulesImmediateNonBlockingPublicationThenStopsWithTheHandler() {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ScheduledFuture<?> future = mock(ScheduledFuture.class);
        when(kafka.send(org.mockito.ArgumentMatchers.<ProducerRecord<String, String>>any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(scheduler.scheduleAtFixedRate(
                org.mockito.ArgumentMatchers.any(Runnable.class), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS)
        )).thenReturn((ScheduledFuture) future);
        WorkerHeartbeatPublisher publisher = new WorkerHeartbeatPublisher(
                kafka,
                JsonMapper.builder().findAndAddModules().build(),
                new WorkerProperties("worker-a", "workers", Duration.ofSeconds(3)),
                Clock.fixed(Instant.parse("2026-09-05T08:00:00Z"), ZoneOffset.UTC),
                new SimpleMeterRegistry(),
                scheduler,
                Duration.ofSeconds(10)
        );
        TaskCommandV1 command = new TaskCommandV1(
                UUID.randomUUID(), UUID.randomUUID(), "ROOT", "NOOP", Map.of(),
                1, 1, UUID.randomUUID(), null
        );

        WorkerHeartbeatPublisher.HeartbeatHandle handle = publisher.start("merchant-a", command);
        ArgumentCaptor<Runnable> scheduled = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduler).scheduleAtFixedRate(scheduled.capture(), eq(0L), eq(10_000L),
                eq(TimeUnit.MILLISECONDS));
        scheduled.getValue().run();
        handle.close();

        ArgumentCaptor<ProducerRecord<String, String>> records = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafka).send(records.capture());
        assertThat(records.getAllValues()).allSatisfy(record -> {
            assertThat(record.topic()).isEqualTo(FlowForgeTopics.TASK_HEARTBEATS_V1);
            assertThat(record.key()).isEqualTo(command.taskExecutionId().toString());
            assertThat(record.value()).contains(command.fencingToken().toString());
            assertThat(record.value()).contains("\"tenantId\":\"merchant-a\"");
            assertThat(record.headers().lastHeader("flowforge-correlation-id")).isNotNull();
            assertThat(record.headers().lastHeader(FlowForgeHeaders.TENANT_ID)).isNotNull();
        });
        verify(future).cancel(false);
        assertThat(command.fencingToken()).isNotNull();
    }
}
