package io.flowforge.worker.application;

import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
public class TaskHandlerRegistry {
    private final Map<String, WorkerTaskHandler> handlers;

    public TaskHandlerRegistry(List<WorkerTaskHandler> handlers) {
        Map<String, WorkerTaskHandler> registered = new LinkedHashMap<>();
        for (WorkerTaskHandler handler : handlers) {
            String type = normalize(handler.taskType());
            if (registered.putIfAbsent(type, handler) != null) {
                throw new IllegalStateException("Multiple worker handlers registered for task type " + type);
            }
        }
        this.handlers = Map.copyOf(registered);
    }

    public WorkerTaskResult execute(TaskExecutionContext context) {
        String type = normalize(context.command().taskType());
        WorkerTaskHandler handler = handlers.get(type);
        if (handler == null) {
            return WorkerTaskResult.failed("UNSUPPORTED_TASK_TYPE", "No worker handler for task type " + type);
        }
        return handler.execute(context);
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("taskType must not be blank");
        return value.trim().toUpperCase(Locale.ROOT);
    }
}
