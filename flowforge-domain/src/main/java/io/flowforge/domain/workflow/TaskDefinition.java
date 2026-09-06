package io.flowforge.domain.workflow;

import java.util.Map;

public record TaskDefinition(
        String key,
        String name,
        String type,
        Map<String, Object> configuration,
        Map<String, SecretReference> secretReferences,
        TaskReliabilityPolicy reliabilityPolicy,
        Integer maxConcurrency
) {
    public TaskDefinition {
        configuration = configuration == null ? Map.of() : Map.copyOf(configuration);
        rejectInlineSecrets(configuration);
        secretReferences = secretReferences == null ? Map.of() : Map.copyOf(secretReferences);
        secretReferences.forEach((binding, reference) -> {
            if (binding == null || !binding.matches("[A-Za-z][A-Za-z0-9_.-]{0,99}")) {
                throw new IllegalArgumentException("secret binding must be a valid identifier");
            }
            if (reference == null) throw new IllegalArgumentException("secret reference must not be null");
        });
        reliabilityPolicy = reliabilityPolicy == null
                ? TaskReliabilityPolicy.defaults()
                : reliabilityPolicy;
    }

    public TaskDefinition(String key, String name, String type, Map<String, Object> configuration) {
        this(key, name, type, configuration, Map.of(), TaskReliabilityPolicy.defaults(), null);
    }

    public TaskDefinition(
            String key,
            String name,
            String type,
            Map<String, Object> configuration,
            TaskReliabilityPolicy reliabilityPolicy
    ) {
        this(key, name, type, configuration, Map.of(), reliabilityPolicy, null);
    }

    public TaskDefinition(
            String key,
            String name,
            String type,
            Map<String, Object> configuration,
            TaskReliabilityPolicy reliabilityPolicy,
            Integer maxConcurrency
    ) {
        this(key, name, type, configuration, Map.of(), reliabilityPolicy, maxConcurrency);
    }

    private static void rejectInlineSecrets(Map<String, ?> values) {
        values.forEach((key, value) -> {
            String normalized = key == null ? "" : key.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
            if (normalized.contains("password") || normalized.contains("secret")
                    || normalized.contains("credential") || normalized.contains("privatekey")
                    || normalized.contains("apikey") || normalized.equals("token")
                    || normalized.endsWith("token")) {
                throw new IllegalArgumentException(
                        "task configuration must not contain inline secrets; use secretReferences for " + key
                );
            }
            if (value instanceof Map<?, ?> nested) {
                @SuppressWarnings("unchecked") Map<String, ?> nestedValues = (Map<String, ?>) nested;
                rejectInlineSecrets(nestedValues);
            }
        });
    }
}
