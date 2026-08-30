package io.flowforge.domain.workflow;

import java.util.Map;
import java.util.Objects;

public record TaskDefinition(
        String key,
        String name,
        String type,
        Map<String, Object> configuration
) {
    public TaskDefinition {
        configuration = configuration == null ? Map.of() : Map.copyOf(configuration);
    }
}
