package io.flowforge.application.execution;

import java.util.UUID;

public final class ExecutionNotFoundException extends RuntimeException {
    public ExecutionNotFoundException(UUID id) {
        super("Workflow execution not found: " + id);
    }
}
