package io.flowforge.application.workflow;

public final class WorkflowConflictException extends RuntimeException {
    public WorkflowConflictException(String message) {
        super(message);
    }
}
