package io.flowforge.domain.execution;

import io.flowforge.domain.workflow.TaskDependency;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class DagResolver {
    private DagResolver() {
    }

    public static Map<String, TaskRunStatus> initialTaskStatuses(
            List<String> taskKeys,
            List<TaskDependency> dependencies
    ) {
        Set<String> dependentTaskKeys = dependencies.stream()
                .map(TaskDependency::taskKey)
                .collect(java.util.stream.Collectors.toSet());
        Map<String, TaskRunStatus> statuses = new LinkedHashMap<>();
        taskKeys.forEach(key -> statuses.put(
                key,
                dependentTaskKeys.contains(key) ? TaskRunStatus.BLOCKED : TaskRunStatus.READY
        ));
        return Map.copyOf(statuses);
    }

    public static Set<String> newlyReadyTasks(
            Map<String, TaskRunStatus> taskStatuses,
            List<TaskDependency> dependencies
    ) {
        Map<String, Set<String>> prerequisites = new LinkedHashMap<>();
        taskStatuses.keySet().forEach(key -> prerequisites.put(key, new LinkedHashSet<>()));
        dependencies.forEach(dependency -> prerequisites
                .computeIfAbsent(dependency.taskKey(), ignored -> new LinkedHashSet<>())
                .add(dependency.dependsOnTaskKey()));

        Set<String> ready = new LinkedHashSet<>();
        taskStatuses.forEach((taskKey, status) -> {
            if (status != TaskRunStatus.BLOCKED) return;
            Set<String> required = prerequisites.getOrDefault(taskKey, Set.of());
            if (!required.isEmpty() && required.stream()
                    .allMatch(key -> taskStatuses.get(key) == TaskRunStatus.SUCCEEDED)) {
                ready.add(taskKey);
            }
        });
        return Set.copyOf(ready);
    }
}
