package io.flowforge.domain.workflow;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public record WorkflowDraft(
        String name,
        String description,
        List<TaskDefinition> tasks,
        List<TaskDependency> dependencies,
        Integer maxConcurrentExecutions
) {
    private static final Pattern KEY_PATTERN = Pattern.compile("[A-Z][A-Z0-9_]{0,99}");
    private static final Pattern TYPE_PATTERN = Pattern.compile("[A-Z][A-Z0-9_.-]{0,99}");

    public WorkflowDraft {
        name = name == null ? null : name.strip();
        description = description == null ? null : description.strip();
        tasks = tasks == null ? List.of() : List.copyOf(tasks);
        dependencies = dependencies == null ? List.of() : List.copyOf(dependencies);
        validate(name, description, tasks, dependencies, maxConcurrentExecutions);
    }

    public WorkflowDraft(
            String name,
            String description,
            List<TaskDefinition> tasks,
            List<TaskDependency> dependencies
    ) {
        this(name, description, tasks, dependencies, null);
    }

    private static void validate(
            String name,
            String description,
            List<TaskDefinition> tasks,
            List<TaskDependency> dependencies,
            Integer maxConcurrentExecutions
    ) {
        List<String> violations = new ArrayList<>();
        if (name == null || name.isBlank()) violations.add("name must not be blank");
        if (name != null && name.length() > 200) violations.add("name must not exceed 200 characters");
        if (description != null && description.length() > 2_000) {
            violations.add("description must not exceed 2000 characters");
        }
        if (tasks.isEmpty()) violations.add("at least one task is required");
        if (tasks.size() > 1_000) violations.add("a workflow cannot contain more than 1000 tasks");
        validateLimit(maxConcurrentExecutions, "maxConcurrentExecutions", violations);

        Set<String> keys = new HashSet<>();
        for (int i = 0; i < tasks.size(); i++) {
            TaskDefinition task = tasks.get(i);
            if (task == null) {
                violations.add("tasks[" + i + "] must not be null");
                continue;
            }
            if (task.key() == null || !KEY_PATTERN.matcher(task.key()).matches()) {
                violations.add("task key must match " + KEY_PATTERN.pattern());
            } else if (!keys.add(task.key())) {
                violations.add("duplicate task key: " + task.key());
            }
            if (task.name() == null || task.name().isBlank()) {
                violations.add("task name must not be blank for " + task.key());
            } else if (task.name().length() > 200) {
                violations.add("task name must not exceed 200 characters for " + task.key());
            }
            if (task.type() == null || !TYPE_PATTERN.matcher(task.type()).matches()) {
                violations.add("task type must match " + TYPE_PATTERN.pattern() + " for " + task.key());
            }
            validateLimit(task.maxConcurrency(), "maxConcurrency for " + task.key(), violations);
        }

        Set<TaskDependency> uniqueDependencies = new HashSet<>();
        for (TaskDependency dependency : dependencies) {
            if (dependency == null) {
                violations.add("dependencies must not contain null");
                continue;
            }
            if (!keys.contains(dependency.taskKey())) {
                violations.add("unknown dependent task: " + dependency.taskKey());
            }
            if (!keys.contains(dependency.dependsOnTaskKey())) {
                violations.add("unknown prerequisite task: " + dependency.dependsOnTaskKey());
            }
            if (dependency.taskKey() != null && dependency.taskKey().equals(dependency.dependsOnTaskKey())) {
                violations.add("task cannot depend on itself: " + dependency.taskKey());
            }
            if (!uniqueDependencies.add(dependency)) {
                violations.add("duplicate dependency: " + dependency.taskKey() + " -> " + dependency.dependsOnTaskKey());
            }
        }

        if (violations.isEmpty() && containsCycle(keys, dependencies)) {
            violations.add("workflow graph must be acyclic");
        }
        if (!violations.isEmpty()) throw new DomainValidationException(violations);
    }

    private static void validateLimit(Integer limit, String name, List<String> violations) {
        if (limit != null && (limit < 1 || limit > 100_000)) {
            violations.add(name + " must be between 1 and 100000");
        }
    }

    private static boolean containsCycle(Set<String> keys, List<TaskDependency> dependencies) {
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> dependents = new HashMap<>();
        keys.forEach(key -> {
            inDegree.put(key, 0);
            dependents.put(key, new ArrayList<>());
        });
        for (TaskDependency dependency : dependencies) {
            inDegree.compute(dependency.taskKey(), (ignored, degree) -> degree + 1);
            dependents.get(dependency.dependsOnTaskKey()).add(dependency.taskKey());
        }

        ArrayDeque<String> ready = new ArrayDeque<>();
        inDegree.forEach((key, degree) -> {
            if (degree == 0) ready.add(key);
        });
        int visited = 0;
        while (!ready.isEmpty()) {
            String key = ready.remove();
            visited++;
            for (String dependent : dependents.get(key)) {
                int remaining = inDegree.compute(dependent, (ignored, degree) -> degree - 1);
                if (remaining == 0) ready.add(dependent);
            }
        }
        return visited != keys.size();
    }
}
