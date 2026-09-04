package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.TaskResult;
import io.flowforge.application.execution.TaskWorkItem;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Component
public class FailTaskHandler implements TaskHandler {
    @Override
    public String taskType() {
        return "FAIL";
    }

    @Override
    public CompletionStage<TaskResult> handle(TaskWorkItem workItem) {
        String errorCode = stringConfiguration(workItem, "errorCode", "DETERMINISTIC_FAILURE");
        String message = stringConfiguration(workItem, "message", "Task configured to fail");
        return CompletableFuture.completedFuture(TaskResult.failed(errorCode, message));
    }

    private static String stringConfiguration(TaskWorkItem workItem, String key, String defaultValue) {
        Object value = workItem.configuration().get(key);
        return value == null ? defaultValue : String.valueOf(value);
    }
}
