package io.flowforge.worker.application;

import io.flowforge.messaging.TaskCommandV1;

import java.util.Objects;
import java.util.UUID;

public record TaskExecutionContext(UUID idempotencyToken, TaskCommandV1 command) {
    public TaskExecutionContext {
        Objects.requireNonNull(idempotencyToken, "idempotencyToken must not be null");
        Objects.requireNonNull(command, "command must not be null");
    }
}
