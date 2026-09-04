package io.flowforge.domain.execution;

import io.flowforge.domain.workflow.TaskDependency;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DagResolverTest {
    @Test
    void identifiesAllRootsForFanOut() {
        Map<String, TaskRunStatus> statuses = DagResolver.initialTaskStatuses(
                List.of("ROOT", "LEFT", "RIGHT"),
                List.of(
                        new TaskDependency("LEFT", "ROOT"),
                        new TaskDependency("RIGHT", "ROOT")
                )
        );

        assertThat(statuses).containsEntry("ROOT", TaskRunStatus.READY)
                .containsEntry("LEFT", TaskRunStatus.BLOCKED)
                .containsEntry("RIGHT", TaskRunStatus.BLOCKED);
    }

    @Test
    void fanInWaitsForEveryPrerequisite() {
        List<TaskDependency> dependencies = List.of(
                new TaskDependency("JOIN", "LEFT"),
                new TaskDependency("JOIN", "RIGHT")
        );
        Map<String, TaskRunStatus> statuses = new LinkedHashMap<>();
        statuses.put("LEFT", TaskRunStatus.SUCCEEDED);
        statuses.put("RIGHT", TaskRunStatus.RUNNING);
        statuses.put("JOIN", TaskRunStatus.BLOCKED);

        assertThat(DagResolver.newlyReadyTasks(statuses, dependencies)).isEmpty();

        statuses.put("RIGHT", TaskRunStatus.SUCCEEDED);
        assertThat(DagResolver.newlyReadyTasks(statuses, dependencies)).containsExactly("JOIN");
    }

    @Test
    void mixedDagOnlyReleasesSatisfiedBranches() {
        List<TaskDependency> dependencies = List.of(
                new TaskDependency("B", "A"),
                new TaskDependency("C", "A"),
                new TaskDependency("D", "B"),
                new TaskDependency("D", "C")
        );
        Map<String, TaskRunStatus> statuses = new LinkedHashMap<>();
        statuses.put("A", TaskRunStatus.SUCCEEDED);
        statuses.put("B", TaskRunStatus.BLOCKED);
        statuses.put("C", TaskRunStatus.BLOCKED);
        statuses.put("D", TaskRunStatus.BLOCKED);

        assertThat(DagResolver.newlyReadyTasks(statuses, dependencies))
                .containsExactlyInAnyOrder("B", "C");
    }
}
