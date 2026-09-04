package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.TaskResult;
import io.flowforge.application.execution.TaskWorkItem;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class DelayTaskHandler implements TaskHandler {
    private static final long MAXIMUM_DELAY_MILLIS = 60_000;

    private final ScheduledExecutorService executor;

    public DelayTaskHandler(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    @Override
    public String taskType() {
        return "DELAY";
    }

    @Override
    public CompletionStage<TaskResult> handle(TaskWorkItem workItem) {
        long delayMillis = delayMillis(workItem.configuration().getOrDefault("durationMs", 0));
        CompletableFuture<TaskResult> future = new CompletableFuture<>();
        executor.schedule(() -> future.complete(TaskResult.succeeded()), delayMillis, TimeUnit.MILLISECONDS);
        return future;
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
