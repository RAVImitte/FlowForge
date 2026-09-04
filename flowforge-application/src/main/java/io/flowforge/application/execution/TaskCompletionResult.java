package io.flowforge.application.execution;

import io.flowforge.domain.execution.WorkflowExecution;

import java.util.Objects;

public record TaskCompletionResult(WorkflowExecution execution, boolean applied) {
    public TaskCompletionResult {
        Objects.requireNonNull(execution, "execution must not be null");
    }
}
