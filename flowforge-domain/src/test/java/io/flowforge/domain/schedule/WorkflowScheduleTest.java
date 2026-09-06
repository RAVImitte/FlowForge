package io.flowforge.domain.schedule;

import io.flowforge.domain.tenancy.TenantId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowScheduleTest {
    @Test
    void normalizesCronAndDefaultsMisfirePolicy() {
        CronSchedule cron = new CronSchedule(" 0 0 * * * * ", ZoneId.of("Asia/Kolkata"));
        WorkflowScheduleDraft draft = new WorkflowScheduleDraft(UUID.randomUUID(), cron, null);

        assertThat(cron.expression()).isEqualTo("0 0 * * * *");
        assertThat(draft.misfirePolicy()).isEqualTo(MisfirePolicy.FIRE_ONCE);
    }

    @Test
    void rejectsIncompleteSpecificationsAndInvalidAggregateTime() {
        assertThatThrownBy(() -> new CronSchedule(" ", ZoneId.of("UTC")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OneTimeSchedule(null))
                .isInstanceOf(NullPointerException.class);
        Instant now = Instant.parse("2026-09-05T12:00:00Z");
        assertThatThrownBy(() -> new WorkflowSchedule(
                UUID.randomUUID(),
                TenantId.LOCAL,
                UUID.randomUUID(),
                new OneTimeSchedule(now.plusSeconds(60)),
                MisfirePolicy.SKIP,
                ScheduleStatus.ACTIVE,
                now.plusSeconds(60),
                0,
                now,
                now.minusSeconds(1)
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void completedSchedulesHaveNoNextFireTime() {
        Instant now = Instant.parse("2026-09-05T12:00:00Z");
        WorkflowSchedule completed = new WorkflowSchedule(
                UUID.randomUUID(),
                TenantId.LOCAL,
                UUID.randomUUID(),
                new OneTimeSchedule(now),
                MisfirePolicy.FIRE_ONCE,
                ScheduleStatus.COMPLETED,
                null,
                1,
                now.minusSeconds(1),
                now
        );

        assertThat(completed.nextFireAt()).isNull();
        assertThatThrownBy(() -> new WorkflowSchedule(
                completed.id(),
                completed.tenantId(),
                completed.workflowId(),
                completed.spec(),
                completed.misfirePolicy(),
                ScheduleStatus.COMPLETED,
                now.plusSeconds(1),
                completed.lockVersion(),
                completed.createdAt(),
                completed.updatedAt()
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("completed schedule");
    }
}
