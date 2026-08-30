package io.flowforge.application.workflow;

import java.util.UUID;

public final class WorkflowNotFoundException extends RuntimeException {
    public WorkflowNotFoundException(UUID id) {
        super("Workflow not found: " + id);
    }
}
