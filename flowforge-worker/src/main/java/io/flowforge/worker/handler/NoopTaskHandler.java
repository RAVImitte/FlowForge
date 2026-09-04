package io.flowforge.worker.handler;

import io.flowforge.worker.application.TaskExecutionContext;
import io.flowforge.worker.application.WorkerTaskHandler;
import io.flowforge.worker.application.WorkerTaskResult;
import org.springframework.stereotype.Component;

@Component
public class NoopTaskHandler implements WorkerTaskHandler {
    @Override
    public String taskType() {
        return "NOOP";
    }

    @Override
    public WorkerTaskResult execute(TaskExecutionContext context) {
        return WorkerTaskResult.succeeded();
    }
}
