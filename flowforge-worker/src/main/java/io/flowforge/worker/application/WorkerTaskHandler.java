package io.flowforge.worker.application;

public interface WorkerTaskHandler {
    String taskType();

    WorkerTaskResult execute(TaskExecutionContext context);
}
