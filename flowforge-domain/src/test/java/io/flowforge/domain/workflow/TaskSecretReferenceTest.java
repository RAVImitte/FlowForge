package io.flowforge.domain.workflow;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskSecretReferenceTest {
    @Test
    void keepsSecretReferencesSeparateFromOrdinaryConfiguration() {
        TaskDefinition task = new TaskDefinition(
                "PAY", "Pay", "PAYMENT", Map.of("endpoint", "https://payments.test"),
                Map.of("apiKey", new SecretReference("vault", "tenants/acme/payment", "7")),
                null, null
        );

        assertThat(task.configuration()).doesNotContainKey("apiKey");
        assertThat(task.secretReferences().get("apiKey").provider()).isEqualTo("vault");
    }

    @Test
    void rejectsInlineSecretsIncludingNestedConfiguration() {
        assertThatThrownBy(() -> new TaskDefinition(
                "PAY", "Pay", "PAYMENT",
                Map.of("headers", Map.of("accessToken", "must-not-be-here"))
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("secretReferences");
    }
}
