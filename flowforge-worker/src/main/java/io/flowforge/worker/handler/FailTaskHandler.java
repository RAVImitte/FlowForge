package io.flowforge.worker.handler;

import io.flowforge.worker.application.TaskExecutionContext;
import io.flowforge.worker.application.WorkerTaskHandler;
import io.flowforge.worker.application.WorkerTaskResult;
import org.springframework.stereotype.Component;

@Component
public class FailTaskHandler implements WorkerTaskHandler {
    @Override
    public String taskType() {
        return "FAIL";
    }

    @Override
    public WorkerTaskResult execute(TaskExecutionContext context) {
        Object errorCode = context.command().configuration().get("errorCode");
        Object message = context.command().configuration().get("message");
        return WorkerTaskResult.failed(
                errorCode == null ? "DETERMINISTIC_FAILURE" : String.valueOf(errorCode),
                message == null ? "Task configured to fail" : String.valueOf(message)
        );
    }
}
