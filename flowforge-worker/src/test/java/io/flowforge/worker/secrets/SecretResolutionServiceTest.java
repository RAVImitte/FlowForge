package io.flowforge.worker.secrets;

import io.flowforge.messaging.SecretReferenceV1;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SecretResolutionServiceTest {
    @Test
    void resolvesBindingsThroughTheSelectedProviderAndClearsTheWorkingSet() {
        SecretProvider vault = new SecretProvider() {
            @Override public String providerId() { return "vault"; }
            @Override public String resolve(String tenantId, SecretReferenceV1 reference) {
                return tenantId + ":resolved";
            }
        };
        SecretResolutionService service = new SecretResolutionService(List.of(vault));

        ResolvedSecrets resolved = service.resolve(
                "merchant-a", Map.of("paymentKey", new SecretReferenceV1("vault", "payment/key", "3"))
        );
        assertThat(resolved.values()).containsEntry("paymentKey", "merchant-a:resolved");
        resolved.close();
        assertThat(resolved.values()).isEmpty();
    }

    @Test
    void rejectsUnknownProvidersWithoutEchoingSecretMaterial() {
        SecretResolutionService service = new SecretResolutionService(List.of());

        assertThatThrownBy(() -> service.resolve(
                "merchant-a", Map.of("key", new SecretReferenceV1("vault", "payment/key", null))
        )).isInstanceOf(SecretResolutionException.class)
                .hasMessageContaining("vault")
                .hasMessageNotContaining("payment/key");
    }
}
