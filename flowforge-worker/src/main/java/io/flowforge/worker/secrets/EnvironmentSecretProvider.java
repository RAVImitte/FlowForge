package io.flowforge.worker.secrets;

import io.flowforge.messaging.SecretReferenceV1;
import org.springframework.stereotype.Component;

@Component
public class EnvironmentSecretProvider implements SecretProvider {
    @Override
    public String providerId() {
        return "env";
    }

    @Override
    public String resolve(String tenantId, SecretReferenceV1 reference) {
        if (reference.version() != null) {
            throw new SecretResolutionException("The env provider does not support versioned references");
        }
        String value = System.getenv(reference.name());
        if (value == null) throw new SecretResolutionException("The referenced environment secret is unavailable");
        return value;
    }
}
