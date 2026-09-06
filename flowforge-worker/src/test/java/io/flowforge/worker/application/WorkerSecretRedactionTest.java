package io.flowforge.worker.application;

import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.SecretReferenceV1;
import io.flowforge.messaging.TaskCommandV1;
import io.flowforge.worker.config.WorkerProperties;
import io.flowforge.worker.messaging.WorkerHeartbeatPublisher;
import io.flowforge.worker.persistence.CommandReceipt;
import io.flowforge.worker.persistence.WorkerCommandRepository;
import io.flowforge.worker.secrets.SecretProvider;
import io.flowforge.worker.secrets.SecretResolutionService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WorkerSecretRedactionTest {
    @Test
    void resolvesOnlyInsideTheWorkerAndRedactsHandlerFailuresBeforePersistence() {
        String material = "super-sensitive-value";
        SecretProvider provider = new SecretProvider() {
            @Override public String providerId() { return "test"; }
            @Override public String resolve(String tenantId, SecretReferenceV1 reference) { return material; }
        };
        SecretResolutionService secrets = new SecretResolutionService(List.of(provider));
        WorkerCommandRepository repository = mock(WorkerCommandRepository.class);
        TaskHandlerRegistry handlers = mock(TaskHandlerRegistry.class);
        WorkerHeartbeatPublisher heartbeats = mock(WorkerHeartbeatPublisher.class);
        when(repository.receive(any(), anyString(), any())).thenReturn(new CommandReceipt(true, false, null));
        when(heartbeats.start(anyString(), any())).thenReturn(() -> { });
        when(handlers.execute(any())).thenAnswer(invocation -> {
            TaskExecutionContext context = invocation.getArgument(0);
            return WorkerTaskResult.failed("REMOTE_FAILURE", "upstream echoed " + context.secrets().get("apiKey"));
        });

        UUID executionId = UUID.randomUUID();
        TaskCommandV1 command = new TaskCommandV1(
                executionId, UUID.randomUUID(), "PAY", "PAYMENT", Map.of("amount", 42),
                Map.of("apiKey", new SecretReferenceV1("test", "payment/key", "4")),
                1, 1, null, null
        );
        MessageEnvelope<TaskCommandV1> envelope = new MessageEnvelope<>(
                UUID.randomUUID(), TaskCommandV1.EVENT_TYPE, TaskCommandV1.SCHEMA_VERSION,
                Instant.parse("2026-09-06T12:00:00Z"), executionId, "merchant-a", command
        );
        WorkerCommandProcessor processor = new WorkerCommandProcessor(
                repository, handlers,
                new WorkerProperties("worker-a", "workers", Duration.ofSeconds(2)),
                Clock.fixed(Instant.parse("2026-09-06T12:00:00Z"), ZoneOffset.UTC),
                new SimpleMeterRegistry(), heartbeats, ObservationRegistry.create(), secrets
        );

        processor.process(envelope, "reference-only-payload");

        ArgumentCaptor<TaskExecutionContext> context = ArgumentCaptor.forClass(TaskExecutionContext.class);
        verify(handlers).execute(context.capture());
        assertThat(context.getValue().secrets()).containsEntry("apiKey", material);
        ArgumentCaptor<WorkerTaskResult> result = ArgumentCaptor.forClass(WorkerTaskResult.class);
        verify(repository).complete(any(), result.capture(), anyString(), any());
        assertThat(result.getValue().errorMessage())
                .contains("[REDACTED]")
                .doesNotContain(material);
        assertThat(command.configuration()).doesNotContainValue(material);
    }
}
