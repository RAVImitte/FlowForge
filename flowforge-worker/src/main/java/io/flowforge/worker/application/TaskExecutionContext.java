package io.flowforge.worker.application;

import io.flowforge.messaging.TaskCommandV1;

import java.util.Objects;
import java.util.Map;
import java.util.UUID;

public record TaskExecutionContext(UUID idempotencyToken, TaskCommandV1 command, Map<String, String> secrets) {
    public TaskExecutionContext {
        Objects.requireNonNull(idempotencyToken, "idempotencyToken must not be null");
        Objects.requireNonNull(command, "command must not be null");
        secrets = secrets == null ? Map.of() : Map.copyOf(secrets);
    }

    public TaskExecutionContext(UUID idempotencyToken, TaskCommandV1 command) {
        this(idempotencyToken, command, Map.of());
    }

    @Override
    public String toString() {
        return "TaskExecutionContext[idempotencyToken=" + idempotencyToken
                + ", command=" + command + ", secretBindings=" + secrets.keySet() + "]";
    }
}
