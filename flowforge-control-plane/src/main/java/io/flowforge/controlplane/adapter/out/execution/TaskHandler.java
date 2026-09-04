package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.TaskResult;
import io.flowforge.application.execution.TaskWorkItem;

import java.util.concurrent.CompletionStage;

public interface TaskHandler {
    String taskType();

    CompletionStage<TaskResult> handle(TaskWorkItem workItem);
}
