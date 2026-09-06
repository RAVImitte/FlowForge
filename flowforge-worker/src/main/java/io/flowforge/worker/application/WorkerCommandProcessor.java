package io.flowforge.worker.application;

import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskCommandV1;
import io.flowforge.worker.config.WorkerProperties;
import io.flowforge.worker.persistence.CommandReceipt;
import io.flowforge.worker.persistence.WorkerCommandRepository;
import io.flowforge.worker.messaging.WorkerHeartbeatPublisher;
import io.flowforge.worker.secrets.ResolvedSecrets;
import io.flowforge.worker.secrets.SecretResolutionException;
import io.flowforge.worker.secrets.SecretResolutionService;
import io.flowforge.observability.SensitiveDataRedactor;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;

@Component
public class WorkerCommandProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerCommandProcessor.class);

    private final WorkerCommandRepository repository;
    private final TaskHandlerRegistry handlers;
    private final WorkerProperties worker;
    private final Clock clock;
    private final MeterRegistry meters;
    private final WorkerHeartbeatPublisher heartbeats;
    private final ObservationRegistry observations;
    private final SecretResolutionService secrets;

    public WorkerCommandProcessor(
            WorkerCommandRepository repository,
            TaskHandlerRegistry handlers,
            WorkerProperties worker,
            Clock clock,
            MeterRegistry meters,
            WorkerHeartbeatPublisher heartbeats,
            ObservationRegistry observations,
            SecretResolutionService secrets
    ) {
        this.repository = repository;
        this.handlers = handlers;
        this.worker = worker;
        this.clock = clock;
        this.meters = meters;
        this.heartbeats = heartbeats;
        this.observations = observations;
        this.secrets = secrets;
    }

    public void process(MessageEnvelope<TaskCommandV1> envelope, String rawPayload) {
        Observation observation = Observation.start("flowforge.worker.task.execute", observations);
        try (Observation.Scope ignored = observation.openScope()) {
            processObserved(envelope, rawPayload, observation);
        } catch (RuntimeException failure) {
            observation.error(failure);
            throw failure;
        } finally {
            observation.stop();
        }
    }

    private void processObserved(
            MessageEnvelope<TaskCommandV1> envelope,
            String rawPayload,
            Observation observation
    ) {
        CommandReceipt receipt = repository.receive(envelope, rawPayload, clock.instant());
        if (receipt.completed()) {
            observation.lowCardinalityKeyValue("flowforge.outcome", "duplicate");
            meters.counter("flowforge.worker.commands.duplicates", "task.type", envelope.payload().taskType())
                    .increment();
            return;
        }

        WorkerTaskResult result;
        try (WorkerHeartbeatPublisher.HeartbeatHandle ignored = heartbeats.start(
                envelope.tenantId(), envelope.payload()
        )) {
            try (ResolvedSecrets resolved = secrets.resolve(
                    envelope.tenantId(), envelope.payload().secretReferences()
            )) {
                try {
                    result = sanitize(
                            handlers.execute(new TaskExecutionContext(
                                    envelope.eventId(), envelope.payload(), resolved.values()
                            )),
                            resolved
                    );
                } catch (RuntimeException failure) {
                    String safeMessage = SensitiveDataRedactor.redact(
                            rootMessage(failure), resolved.valuesForRedaction()
                    );
                    observation.lowCardinalityKeyValue("flowforge.error.type", failure.getClass().getSimpleName());
                    LOGGER.warn("Worker handler failed for command {}: {}", envelope.eventId(), safeMessage);
                    result = WorkerTaskResult.failed("HANDLER_EXCEPTION", safeMessage);
                }
            }
        } catch (SecretResolutionException failure) {
            observation.lowCardinalityKeyValue("flowforge.error.type", "SecretResolutionException");
            LOGGER.warn("Secret resolution failed for command {}", envelope.eventId());
            result = WorkerTaskResult.failed(
                    "SECRET_RESOLUTION_FAILED", "One or more task secrets could not be resolved"
            );
        } catch (RuntimeException failure) {
            observation.lowCardinalityKeyValue("flowforge.error.type", failure.getClass().getSimpleName());
            LOGGER.warn("Worker execution infrastructure failed for command {}", envelope.eventId());
            result = WorkerTaskResult.failed("WORKER_INFRASTRUCTURE_FAILURE", SensitiveDataRedactor.redact(rootMessage(failure)));
        }
        repository.complete(envelope, result, worker.id(), clock.instant());
        observation.lowCardinalityKeyValue("flowforge.outcome", result.outcome().name().toLowerCase());
        meters.counter(
                "flowforge.worker.commands.completed",
                "task.type", envelope.payload().taskType(),
                "outcome", result.outcome().name()
        ).increment();
    }

    private static String rootMessage(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null) current = current.getCause();
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }

    private static WorkerTaskResult sanitize(WorkerTaskResult result, ResolvedSecrets secrets) {
        return new WorkerTaskResult(
                result.outcome(),
                result.errorCode(),
                SensitiveDataRedactor.redact(result.errorMessage(), secrets.valuesForRedaction()),
                result.retryable()
        );
    }
}
