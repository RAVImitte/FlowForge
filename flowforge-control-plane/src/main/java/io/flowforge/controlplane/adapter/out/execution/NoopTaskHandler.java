package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.TaskResult;
import io.flowforge.application.execution.TaskWorkItem;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Component
public class NoopTaskHandler implements TaskHandler {
    @Override
    public String taskType() {
        return "NOOP";
    }

    @Override
    public CompletionStage<TaskResult> handle(TaskWorkItem workItem) {
        return CompletableFuture.completedFuture(TaskResult.succeeded());
    }
}
