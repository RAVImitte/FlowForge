package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.TaskDependency;
import io.flowforge.domain.workflow.TaskReliabilityPolicy;
import io.flowforge.domain.workflow.WorkflowDefinition;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public record WorkflowResponse(
        UUID id,
        long lockVersion,
        String lifecycleStatus,
        int definitionVersion,
        String versionStatus,
        String name,
        String description,
        List<TaskResponse> tasks,
        List<DependencyResponse> dependencies,
        Instant createdAt,
        Instant updatedAt,
        Instant publishedAt
) {
    static WorkflowResponse from(WorkflowDefinition workflow) {
        return new WorkflowResponse(
                workflow.id(),
                workflow.lockVersion(),
                workflow.lifecycleStatus().name(),
                workflow.definitionVersion(),
                workflow.versionStatus().name(),
                workflow.name(),
                workflow.description(),
                workflow.tasks().stream().map(TaskResponse::from).toList(),
                workflow.dependencies().stream().map(DependencyResponse::from).toList(),
                workflow.createdAt(),
                workflow.updatedAt(),
                workflow.publishedAt()
        );
    }

    public record TaskResponse(
            String key,
            String name,
            String type,
            Map<String, Object> configuration,
            ReliabilityPolicyResponse reliabilityPolicy
    ) {
        static TaskResponse from(TaskDefinition task) {
            return new TaskResponse(
                    task.key(),
                    task.name(),
                    task.type(),
                    task.configuration(),
                    task.reliabilityPolicy().isDefault()
                            ? null
                            : ReliabilityPolicyResponse.from(task.reliabilityPolicy())
            );
        }
    }

    public record ReliabilityPolicyResponse(
            int maxAttempts,
            long initialBackoffMs,
            double backoffMultiplier,
            long maxBackoffMs,
            double jitterFactor,
            Long attemptTimeoutMs,
            java.util.Set<String> retryableErrorCodes
    ) {
        static ReliabilityPolicyResponse from(TaskReliabilityPolicy policy) {
            return new ReliabilityPolicyResponse(
                    policy.maxAttempts(),
                    policy.initialBackoff().toMillis(),
                    policy.backoffMultiplier(),
                    policy.maxBackoff().toMillis(),
                    policy.jitterFactor(),
                    policy.attemptTimeout() == null ? null : policy.attemptTimeout().toMillis(),
                    policy.retryableErrorCodes()
            );
        }
    }

    public record DependencyResponse(String taskKey, String dependsOnTaskKey) {
        static DependencyResponse from(TaskDependency dependency) {
            return new DependencyResponse(dependency.taskKey(), dependency.dependsOnTaskKey());
        }
    }
}
