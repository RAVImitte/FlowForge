package io.flowforge.worker.secrets;

import io.flowforge.messaging.SecretReferenceV1;

public interface SecretProvider {
    String providerId();

    String resolve(String tenantId, SecretReferenceV1 reference);
}
