package io.flowforge.messaging;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TaskSecretReferenceContractTest {
    @Test
    void serializesReferencesButHasNoFieldForResolvedMaterial() throws Exception {
        TaskCommandV1 command = new TaskCommandV1(
                UUID.randomUUID(), UUID.randomUUID(), "PAY", "PAYMENT", Map.of("amount", 42),
                Map.of("apiKey", new SecretReferenceV1("vault", "tenants/acme/payment", "5")),
                1, 1, null, null
        );

        String json = JsonMapper.builder().build().writeValueAsString(command);

        assertThat(json)
                .contains("secretReferences", "tenants/acme/payment", "vault")
                .doesNotContain("resolved", "secretValue", "material");
    }
}
