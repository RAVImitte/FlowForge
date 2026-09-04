package io.flowforge.application.execution;

import java.util.UUID;

public final class WorkflowNotPublishedException extends RuntimeException {
    public WorkflowNotPublishedException(UUID workflowId) {
        super("Workflow has no published version: " + workflowId);
    }
}
