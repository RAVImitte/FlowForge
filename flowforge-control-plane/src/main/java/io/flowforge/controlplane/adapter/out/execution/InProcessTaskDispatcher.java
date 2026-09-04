package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.TaskDispatcher;
import io.flowforge.application.execution.TaskResult;
import io.flowforge.application.execution.TaskWorkItem;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Component
public class InProcessTaskDispatcher implements TaskDispatcher {
    private final Map<String, TaskHandler> handlers;

    public InProcessTaskDispatcher(List<TaskHandler> handlers) {
        Map<String, TaskHandler> byType = new HashMap<>();
        handlers.forEach(handler -> {
            if (byType.put(handler.taskType(), handler) != null) {
                throw new IllegalStateException("Duplicate task handler for type " + handler.taskType());
            }
        });
        this.handlers = Map.copyOf(byType);
    }

    @Override
    public CompletionStage<TaskResult> dispatch(TaskWorkItem workItem) {
        TaskHandler handler = handlers.get(workItem.taskType());
        if (handler == null) {
            return CompletableFuture.completedFuture(TaskResult.failed(
                    "UNSUPPORTED_TASK_TYPE",
                    "No task handler is registered for " + workItem.taskType()
            ));
        }
        return handler.handle(workItem);
    }
}
