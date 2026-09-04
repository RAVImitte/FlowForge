package io.flowforge.application.execution;

import java.util.concurrent.CompletionStage;

public interface TaskDispatcher {
    CompletionStage<TaskResult> dispatch(TaskWorkItem workItem);
}
