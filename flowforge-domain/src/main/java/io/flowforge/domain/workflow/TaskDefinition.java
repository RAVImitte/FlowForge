package io.flowforge.domain.workflow;

import java.util.Map;

public record TaskDefinition(
        String key,
        String name,
        String type,
        Map<String, Object> configuration,
        TaskReliabilityPolicy reliabilityPolicy
) {
    public TaskDefinition {
        configuration = configuration == null ? Map.of() : Map.copyOf(configuration);
        reliabilityPolicy = reliabilityPolicy == null
                ? TaskReliabilityPolicy.defaults()
                : reliabilityPolicy;
    }

    public TaskDefinition(String key, String name, String type, Map<String, Object> configuration) {
        this(key, name, type, configuration, TaskReliabilityPolicy.defaults());
    }
}
