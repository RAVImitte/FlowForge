package io.flowforge.domain.workflow;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record WorkflowDefinition(
        UUID id,
        long lockVersion,
        WorkflowLifecycleStatus lifecycleStatus,
        int definitionVersion,
        WorkflowVersionStatus versionStatus,
        String name,
        String description,
        List<TaskDefinition> tasks,
        List<TaskDependency> dependencies,
        Instant createdAt,
        Instant updatedAt,
        Instant publishedAt
) {
    public WorkflowDefinition {
        tasks = List.copyOf(tasks);
        dependencies = List.copyOf(dependencies);
    }
}
