package io.flowforge.application.execution;

public final class ExecutionConflictException extends RuntimeException {
    public ExecutionConflictException(String message) {
        super(message);
    }
}
