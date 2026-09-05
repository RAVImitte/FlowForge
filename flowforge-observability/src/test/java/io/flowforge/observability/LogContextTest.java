package io.flowforge.observability;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LogContextTest {
    @AfterEach
    void clearContext() {
        MDC.clear();
    }

    @Test
    void restoresTheCompletePreviousContextAfterNestedScopes() {
        MDC.put("existing", "outer");

        try (LogContext ignored = LogContext.open("existing", "operation", "event_id", 42)) {
            assertThat(MDC.get("existing")).isEqualTo("operation");
            assertThat(MDC.get("event_id")).isEqualTo("42");
            try (LogContext nested = LogContext.open("task_key", "ROOT")) {
                assertThat(MDC.get("task_key")).isEqualTo("ROOT");
            }
            assertThat(MDC.get("task_key")).isNull();
            assertThat(MDC.get("event_id")).isEqualTo("42");
        }

        assertThat(MDC.getCopyOfContextMap()).containsExactlyEntriesOf(java.util.Map.of("existing", "outer"));
    }

    @Test
    void rejectsMalformedPairsWithoutChangingTheContext() {
        MDC.put("existing", "value");

        assertThatThrownBy(() -> LogContext.open("missing-value"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LogContext.open(1, "value"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LogContext.open("", "value"))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(MDC.getCopyOfContextMap()).containsExactlyEntriesOf(java.util.Map.of("existing", "value"));
    }
}
