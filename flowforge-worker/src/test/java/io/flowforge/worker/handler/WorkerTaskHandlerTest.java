package io.flowforge.worker.handler;

import io.flowforge.messaging.TaskCommandV1;
import io.flowforge.messaging.TaskResultOutcomeV1;
import io.flowforge.worker.application.TaskExecutionContext;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerTaskHandlerTest {
    @Test
    void noopSucceeds() {
        assertThat(new NoopTaskHandler().execute(context("NOOP", Map.of())).outcome())
                .isEqualTo(TaskResultOutcomeV1.SUCCEEDED);
    }

    @Test
    void delayValidatesBoundsAndSucceeds() {
        DelayTaskHandler handler = new DelayTaskHandler();

        assertThat(handler.execute(context("DELAY", Map.of("durationMs", 1))).outcome())
                .isEqualTo(TaskResultOutcomeV1.SUCCEEDED);
        assertThatThrownBy(() -> handler.execute(context("DELAY", Map.of("durationMs", 60_001))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void failUsesConfiguredError() {
        var result = new FailTaskHandler().execute(context(
                "FAIL",
                Map.of("errorCode", "PAYMENT_DECLINED", "message", "Card declined")
        ));

        assertThat(result.outcome()).isEqualTo(TaskResultOutcomeV1.FAILED);
        assertThat(result.errorCode()).isEqualTo("PAYMENT_DECLINED");
        assertThat(result.errorMessage()).isEqualTo("Card declined");
    }

    private static TaskExecutionContext context(String type, Map<String, Object> configuration) {
        UUID workflowId = UUID.randomUUID();
        return new TaskExecutionContext(
                UUID.randomUUID(),
                new TaskCommandV1(
                        workflowId,
                        UUID.randomUUID(),
                        "TASK",
                        type,
                        configuration,
                        1,
                        1
                )
        );
    }
}
