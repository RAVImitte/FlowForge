package io.flowforge.worker.application;

import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskCommandV1;
import io.flowforge.worker.config.WorkerProperties;
import io.flowforge.worker.persistence.CommandReceipt;
import io.flowforge.worker.persistence.WorkerCommandRepository;
import io.flowforge.worker.messaging.WorkerHeartbeatPublisher;
import io.micrometer.core.instrument.MeterRegistry;
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

    public WorkerCommandProcessor(
            WorkerCommandRepository repository,
            TaskHandlerRegistry handlers,
            WorkerProperties worker,
            Clock clock,
            MeterRegistry meters,
            WorkerHeartbeatPublisher heartbeats
    ) {
        this.repository = repository;
        this.handlers = handlers;
        this.worker = worker;
        this.clock = clock;
        this.meters = meters;
        this.heartbeats = heartbeats;
    }

    public void process(MessageEnvelope<TaskCommandV1> envelope, String rawPayload) {
        CommandReceipt receipt = repository.receive(envelope, rawPayload, clock.instant());
        if (receipt.completed()) {
            meters.counter("flowforge.worker.commands.duplicates", "task.type", envelope.payload().taskType())
                    .increment();
            return;
        }

        WorkerTaskResult result;
        try (WorkerHeartbeatPublisher.HeartbeatHandle ignored = heartbeats.start(envelope.payload())) {
            result = handlers.execute(new TaskExecutionContext(envelope.eventId(), envelope.payload()));
        } catch (RuntimeException failure) {
            LOGGER.warn("Worker handler failed for command {}", envelope.eventId(), failure);
            result = WorkerTaskResult.failed("HANDLER_EXCEPTION", rootMessage(failure));
        }
        repository.complete(envelope, result, worker.id(), clock.instant());
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
}
