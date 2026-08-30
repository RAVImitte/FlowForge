package io.flowforge.domain.workflow;

public record TaskDependency(String taskKey, String dependsOnTaskKey) {
}
