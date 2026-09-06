package io.flowforge.domain.workflow;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public record WorkflowDefinition(
        UUID id,
        TenantId tenantId,
        long lockVersion,
        WorkflowLifecycleStatus lifecycleStatus,
        int definitionVersion,
        WorkflowVersionStatus versionStatus,
        String name,
        String description,
        Integer maxConcurrentExecutions,
        List<TaskDefinition> tasks,
        List<TaskDependency> dependencies,
        Instant createdAt,
        Instant updatedAt,
        Instant publishedAt
) {
    public WorkflowDefinition {
        tenantId = Objects.requireNonNull(tenantId, "tenantId");
        tasks = List.copyOf(tasks);
        dependencies = List.copyOf(dependencies);
    }

}
