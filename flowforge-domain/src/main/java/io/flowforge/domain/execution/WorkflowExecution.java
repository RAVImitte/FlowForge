package io.flowforge.domain.execution;

import java.util.List;
import java.util.Objects;

public record WorkflowExecution(
        WorkflowRun workflow,
        List<TaskRun> tasks,
        List<TaskAttempt> attempts,
        List<ExecutionEvent> events
) {
    public WorkflowExecution {
        Objects.requireNonNull(workflow, "workflow must not be null");
        tasks = List.copyOf(tasks);
        attempts = List.copyOf(attempts);
        events = List.copyOf(events);
    }
}
