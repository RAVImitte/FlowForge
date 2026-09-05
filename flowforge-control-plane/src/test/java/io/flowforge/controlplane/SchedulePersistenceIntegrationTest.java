package io.flowforge.controlplane;

import io.flowforge.application.execution.WorkflowNotPublishedException;
import io.flowforge.application.schedule.ScheduleConflictException;
import io.flowforge.application.schedule.ScheduleNotFoundException;
import io.flowforge.application.schedule.WorkflowScheduleService;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.domain.schedule.CronSchedule;
import io.flowforge.domain.schedule.MisfirePolicy;
import io.flowforge.domain.schedule.OneTimeSchedule;
import io.flowforge.domain.schedule.ScheduleStatus;
import io.flowforge.domain.schedule.WorkflowSchedule;
import io.flowforge.domain.schedule.WorkflowScheduleDraft;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
class SchedulePersistenceIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    WorkflowService workflows;

    @Autowired
    WorkflowScheduleService schedules;

    @Test
    void persistsMutatesAndSoftDeletesSchedulesWithOptimisticConcurrency() {
        WorkflowDefinition workflow = publishedWorkflow("Schedule target");
        Instant fireAt = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MICROS);
        WorkflowSchedule created = schedules.create(new WorkflowScheduleDraft(
                workflow.id(), new OneTimeSchedule(fireAt), MisfirePolicy.SKIP
        ));

        assertThat(schedules.get(created.id())).isEqualTo(created);
        assertThat(created.nextFireAt()).isEqualTo(fireAt);
        assertThat(created.status()).isEqualTo(ScheduleStatus.ACTIVE);
        assertThat(schedules.list(0, 20).items()).contains(created);

        WorkflowSchedule paused = schedules.pause(created.id(), created.lockVersion());
        assertThat(paused.status()).isEqualTo(ScheduleStatus.PAUSED);
        assertThat(paused.lockVersion()).isEqualTo(1);
        assertThatThrownBy(() -> schedules.resume(created.id(), created.lockVersion()))
                .isInstanceOf(ScheduleConflictException.class);

        WorkflowSchedule changed = schedules.update(
                paused.id(),
                paused.lockVersion(),
                new WorkflowScheduleDraft(
                        workflow.id(),
                        new CronSchedule("0 0 * * * *", ZoneId.of("UTC")),
                        MisfirePolicy.FIRE_ONCE
                )
        );
        assertThat(changed.spec()).isInstanceOf(CronSchedule.class);
        assertThat(changed.status()).isEqualTo(ScheduleStatus.PAUSED);

        WorkflowSchedule resumed = schedules.resume(changed.id(), changed.lockVersion());
        assertThat(resumed.status()).isEqualTo(ScheduleStatus.ACTIVE);
        assertThat(resumed.nextFireAt()).isAfter(Instant.now());

        schedules.delete(resumed.id(), resumed.lockVersion());
        assertThatThrownBy(() -> schedules.get(resumed.id())).isInstanceOf(ScheduleNotFoundException.class);
        assertThat(schedules.list(0, 20).items()).doesNotContain(resumed);
    }

    @Test
    void requiresAnActiveWorkflowWithAPublishedVersion() {
        WorkflowDefinition draft = workflows.create(workflowDraft("Unpublished"));

        assertThatThrownBy(() -> schedules.create(new WorkflowScheduleDraft(
                draft.id(), new OneTimeSchedule(Instant.now().plusSeconds(60)), null
        ))).isInstanceOf(WorkflowNotPublishedException.class);
    }

    private WorkflowDefinition publishedWorkflow(String name) {
        WorkflowDefinition draft = workflows.create(workflowDraft(name));
        return workflows.publish(draft.id(), draft.lockVersion());
    }

    private static WorkflowDraft workflowDraft(String name) {
        return new WorkflowDraft(
                name,
                null,
                List.of(new TaskDefinition("ROOT", "Root", "NOOP", Map.of())),
                List.of()
        );
    }
}
