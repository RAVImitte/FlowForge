package io.flowforge.controlplane.adapter.out.execution;

import io.flowforge.application.execution.TaskOutcome;
import io.flowforge.application.execution.TaskResult;
import io.flowforge.application.execution.TaskWorkItem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class InProcessTaskDispatcherTest {
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void shutDownExecutor() {
        executor.shutdownNow();
    }

    @Test
    void dispatchesNoopTasksSuccessfully() throws Exception {
        InProcessTaskDispatcher dispatcher = dispatcher();

        TaskResult result = dispatcher.dispatch(workItem("NOOP", Map.of()))
                .toCompletableFuture().get(1, TimeUnit.SECONDS);

        assertThat(result.outcome()).isEqualTo(TaskOutcome.SUCCEEDED);
    }

    @Test
    void returnsConfiguredDeterministicFailures() throws Exception {
        InProcessTaskDispatcher dispatcher = dispatcher();

        TaskResult result = dispatcher.dispatch(workItem(
                        "FAIL",
                        Map.of("errorCode", "PAYMENT_DECLINED", "message", "Card declined")
                ))
                .toCompletableFuture().get(1, TimeUnit.SECONDS);

        assertThat(result.outcome()).isEqualTo(TaskOutcome.FAILED);
        assertThat(result.errorCode()).isEqualTo("PAYMENT_DECLINED");
    }

    @Test
    void completesDelayTasksAsynchronously() throws Exception {
        InProcessTaskDispatcher dispatcher = dispatcher();

        TaskResult result = dispatcher.dispatch(workItem("DELAY", Map.of("durationMs", 10)))
                .toCompletableFuture().get(1, TimeUnit.SECONDS);

        assertThat(result.outcome()).isEqualTo(TaskOutcome.SUCCEEDED);
    }

    @Test
    void failsUnsupportedTaskTypesDeterministically() throws Exception {
        InProcessTaskDispatcher dispatcher = dispatcher();

        TaskResult result = dispatcher.dispatch(workItem("UNKNOWN", Map.of()))
                .toCompletableFuture().get(1, TimeUnit.SECONDS);

        assertThat(result.outcome()).isEqualTo(TaskOutcome.FAILED);
        assertThat(result.errorCode()).isEqualTo("UNSUPPORTED_TASK_TYPE");
    }

    private InProcessTaskDispatcher dispatcher() {
        return new InProcessTaskDispatcher(List.of(
                new NoopTaskHandler(),
                new FailTaskHandler(),
                new DelayTaskHandler(executor)
        ));
    }

    private static TaskWorkItem workItem(String type, Map<String, Object> configuration) {
        return new TaskWorkItem(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "TASK",
                type,
                configuration,
                1,
                1
        );
    }
}
