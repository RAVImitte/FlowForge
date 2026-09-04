package io.flowforge.worker.handler;

import io.flowforge.worker.application.TaskExecutionContext;
import io.flowforge.worker.application.WorkerTaskHandler;
import io.flowforge.worker.application.WorkerTaskResult;
import org.springframework.stereotype.Component;

@Component
public class DelayTaskHandler implements WorkerTaskHandler {
    private static final long MAXIMUM_DELAY_MILLIS = 60_000;

    @Override
    public String taskType() {
        return "DELAY";
    }

    @Override
    public WorkerTaskResult execute(TaskExecutionContext context) {
        long delayMillis = delayMillis(context.command().configuration().getOrDefault("durationMs", 0));
        try {
            Thread.sleep(delayMillis);
            return WorkerTaskResult.succeeded();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("DELAY task was interrupted", exception);
        }
    }

    private static long delayMillis(Object value) {
        long delay;
        try {
            delay = value instanceof Number number ? number.longValue() : Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("DELAY durationMs must be an integer", exception);
        }
        if (delay < 0 || delay > MAXIMUM_DELAY_MILLIS) {
            throw new IllegalArgumentException("DELAY durationMs must be between 0 and 60000");
        }
        return delay;
    }
}
