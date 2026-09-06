package io.flowforge.worker.secrets;

import io.flowforge.messaging.SecretReferenceV1;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
public class SecretResolutionService {
    private final Map<String, SecretProvider> providers;

    public SecretResolutionService(List<SecretProvider> providers) {
        Map<String, SecretProvider> registered = new LinkedHashMap<>();
        for (SecretProvider provider : providers) {
            String id = provider.providerId().strip().toLowerCase(Locale.ROOT);
            if (registered.putIfAbsent(id, provider) != null) {
                throw new IllegalStateException("Multiple secret providers registered for " + id);
            }
        }
        this.providers = Map.copyOf(registered);
    }

    public ResolvedSecrets resolve(String tenantId, Map<String, SecretReferenceV1> references) {
        Map<String, String> resolved = new LinkedHashMap<>();
        try {
            references.forEach((binding, reference) -> {
                SecretProvider provider = providers.get(reference.provider());
                if (provider == null) {
                    throw new SecretResolutionException("No worker secret provider is registered for " + reference.provider());
                }
                try {
                    String value = provider.resolve(tenantId, reference);
                    if (value == null || value.isEmpty()) {
                        throw new SecretResolutionException("Secret provider returned an empty value");
                    }
                    resolved.put(binding, value);
                } catch (SecretResolutionException failure) {
                    throw failure;
                } catch (RuntimeException failure) {
                    throw new SecretResolutionException("Secret provider failed to resolve a reference", failure);
                }
            });
            return new ResolvedSecrets(resolved);
        } catch (RuntimeException failure) {
            resolved.clear();
            throw failure;
        }
    }
}
