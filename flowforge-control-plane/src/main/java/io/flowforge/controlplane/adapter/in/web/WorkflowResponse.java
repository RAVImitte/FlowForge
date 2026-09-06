package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.TaskDependency;
import io.flowforge.domain.workflow.TaskReliabilityPolicy;
import io.flowforge.domain.workflow.SecretReference;
import io.flowforge.domain.workflow.WorkflowDefinition;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public record WorkflowResponse(
        UUID id,
        String tenantId,
        long lockVersion,
        String lifecycleStatus,
        int definitionVersion,
        String versionStatus,
        String name,
        String description,
        Integer maxConcurrentExecutions,
        List<TaskResponse> tasks,
        List<DependencyResponse> dependencies,
        Instant createdAt,
        Instant updatedAt,
        Instant publishedAt
) {
    static WorkflowResponse from(WorkflowDefinition workflow) {
        return new WorkflowResponse(
                workflow.id(),
                workflow.tenantId().value(),
                workflow.lockVersion(),
                workflow.lifecycleStatus().name(),
                workflow.definitionVersion(),
                workflow.versionStatus().name(),
                workflow.name(),
                workflow.description(),
                workflow.maxConcurrentExecutions(),
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
            Map<String, SecretReferenceResponse> secretReferences,
            ReliabilityPolicyResponse reliabilityPolicy,
            Integer maxConcurrency
    ) {
        static TaskResponse from(TaskDefinition task) {
            return new TaskResponse(
                    task.key(),
                    task.name(),
                    task.type(),
                    task.configuration(),
                    task.secretReferences().entrySet().stream().collect(
                            java.util.stream.Collectors.toUnmodifiableMap(
                                    Map.Entry::getKey,
                                    entry -> SecretReferenceResponse.from(entry.getValue())
                            )
                    ),
                    task.reliabilityPolicy().isDefault()
                            ? null
                            : ReliabilityPolicyResponse.from(task.reliabilityPolicy()),
                    task.maxConcurrency()
            );
        }
    }

    public record SecretReferenceResponse(String provider, String name, String version) {
        static SecretReferenceResponse from(SecretReference reference) {
            return new SecretReferenceResponse(reference.provider(), reference.name(), reference.version());
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
