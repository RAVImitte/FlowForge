package io.flowforge.domain.workflow;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class WorkflowDraftTest {
    private static TaskDefinition task(String key) {
        return new TaskDefinition(key, key, "NOOP", Map.of());
    }

    @Test
    void acceptsAValidDag() {
        assertDoesNotThrow(() -> new WorkflowDraft(
                "Order processing",
                null,
                List.of(task("VALIDATE"), task("PAY"), task("NOTIFY")),
                List.of(
                        new TaskDependency("PAY", "VALIDATE"),
                        new TaskDependency("NOTIFY", "PAY")
                )
        ));
    }

    @Test
    void rejectsCycles() {
        assertThatThrownBy(() -> new WorkflowDraft(
                "Cyclic",
                null,
                List.of(task("A"), task("B"), task("C")),
                List.of(
                        new TaskDependency("B", "A"),
                        new TaskDependency("C", "B"),
                        new TaskDependency("A", "C")
                )
        )).isInstanceOf(DomainValidationException.class)
          .satisfies(error -> {
              DomainValidationException validation = (DomainValidationException) error;
              org.assertj.core.api.Assertions.assertThat(validation.violations())
                      .contains("workflow graph must be acyclic");
          });
    }

    @Test
    void rejectsDuplicateKeysAndUnknownDependencies() {
        assertThatThrownBy(() -> new WorkflowDraft(
                "Invalid",
                null,
                List.of(task("A"), task("A")),
                List.of(new TaskDependency("MISSING", "A"))
        )).isInstanceOf(DomainValidationException.class);
    }
}
