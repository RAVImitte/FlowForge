package io.flowforge.controlplane;

import io.flowforge.application.schedule.ClaimedScheduleFire;
import io.flowforge.application.schedule.ScheduleFireRepository;
import io.flowforge.application.schedule.ScheduleFireRunResult;
import io.flowforge.application.schedule.ScheduleFireService;
import io.flowforge.application.schedule.ScheduleMaterializationResult;
import io.flowforge.application.schedule.QueueSnapshot;
import io.flowforge.application.schedule.WorkflowScheduleService;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.application.coordination.TokenBucketPolicy;
import io.flowforge.application.tenancy.TenantQuotaPolicy;
import io.flowforge.application.tenancy.TenantQuotaService;
import io.flowforge.domain.schedule.CronSchedule;
import io.flowforge.domain.schedule.MisfirePolicy;
import io.flowforge.domain.schedule.OneTimeSchedule;
import io.flowforge.domain.schedule.ScheduleStatus;
import io.flowforge.domain.schedule.WorkflowSchedule;
import io.flowforge.domain.schedule.WorkflowScheduleDraft;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import io.flowforge.domain.tenancy.TenantId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "flowforge.scheduling.enabled=false",
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false",
        "flowforge.leases.reaper-enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
class ScheduleTriggerIntegrationTest {
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

    @Autowired
    ScheduleFireService fireService;

    @Autowired
    ScheduleFireRepository fireRepository;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    TenantQuotaService quotas;

    @Test
    void appliesIndependentPendingFireQuotasFromPostgres() {
        jdbc.sql("DELETE FROM workflow_schedule_trigger WHERE status IN ('PENDING', 'PROCESSING')")
                .update();
        jdbc.sql("UPDATE workflow_schedule SET status = 'PAUSED' WHERE status = 'ACTIVE'")
                .update();
        TenantId tenantA = registerTenant("schedule-quota-a", "Schedule Quota A");
        TenantId tenantB = registerTenant("schedule-quota-b", "Schedule Quota B");
        quotas.update(tenantA, quotaPolicy(1), 0);
        quotas.update(tenantB, quotaPolicy(2), 0);
        WorkflowDefinition workflowA = publishedWorkflow(tenantA, "Schedule quota A");
        WorkflowDefinition workflowB = publishedWorkflow(tenantB, "Schedule quota B");
        WorkflowSchedule a1 = oneTimeSchedule(tenantA, workflowA.id(), MisfirePolicy.FIRE_ONCE);
        WorkflowSchedule a2 = oneTimeSchedule(tenantA, workflowA.id(), MisfirePolicy.FIRE_ONCE);
        WorkflowSchedule b1 = oneTimeSchedule(tenantB, workflowB.id(), MisfirePolicy.FIRE_ONCE);
        WorkflowSchedule b2 = oneTimeSchedule(tenantB, workflowB.id(), MisfirePolicy.FIRE_ONCE);
        Instant now = now();
        List.of(a1, a2, b1, b2).forEach(schedule -> makeOneTimeDue(schedule.id(), now));

        ScheduleMaterializationResult result = fireRepository.materializeDue(
                4, now.plusSeconds(1), Duration.ofMinutes(1)
        );

        assertThat(result.pending()).isEqualTo(3);
        assertThat(result.capacityDeferred()).isEqualTo(1);
        assertThat(fireRepository.pendingQueue(tenantA, now.plusSeconds(1)).depth()).isEqualTo(1);
        assertThat(fireRepository.pendingQueue(tenantB, now.plusSeconds(1)).depth()).isEqualTo(2);
    }

    @Test
    void isolatesTriggerLeasesAndPendingCapacityByTenant() {
        jdbc.sql("DELETE FROM workflow_schedule_trigger WHERE status IN ('PENDING', 'PROCESSING')")
                .update();
        jdbc.sql("UPDATE workflow_schedule SET status = 'PAUSED' WHERE status = 'ACTIVE'")
                .update();
        TenantId tenantA = registerTenant("trigger-merchant-a", "Trigger Merchant A");
        TenantId tenantB = registerTenant("trigger-merchant-b", "Trigger Merchant B");
        WorkflowDefinition workflowA = publishedWorkflow(tenantA, "Tenant A trigger");
        WorkflowDefinition workflowB = publishedWorkflow(tenantB, "Tenant B trigger");
        WorkflowSchedule scheduleA = oneTimeSchedule(
                tenantA, workflowA.id(), MisfirePolicy.FIRE_ONCE);
        WorkflowSchedule scheduleB = oneTimeSchedule(
                tenantB, workflowB.id(), MisfirePolicy.FIRE_ONCE);
        Instant due = now().minusSeconds(1);
        makeOneTimeDue(scheduleA.id(), due);
        makeOneTimeDue(scheduleB.id(), due.plusNanos(1_000));

        ScheduleMaterializationResult result = fireRepository.materializeDue(
                10, 1, now(), Duration.ofMinutes(1));

        assertThat(result.pending()).isEqualTo(2);
        assertThat(result.capacityDeferred()).isZero();
        assertThat(fireRepository.pendingQueue(tenantA, now()).depth()).isEqualTo(1);
        assertThat(fireRepository.pendingQueue(tenantB, now()).depth()).isEqualTo(1);
        assertThat(fireRepository.pendingTenants(now(), 10)).contains(tenantA, tenantB);

        ClaimedScheduleFire claimedA = fireRepository.claimPending(
                tenantA, 1, "tenant-a-scheduler", now(), Duration.ofSeconds(30)
        ).getFirst();
        assertThat(claimedA.tenantId()).isEqualTo(tenantA);
        assertThat(fireRepository.markFailed(
                tenantB, claimedA.triggerId(), claimedA.claimToken(), "foreign", now()
        )).isFalse();
        assertThat(fireRepository.markFailed(
                tenantA, claimedA.triggerId(), claimedA.claimToken(), "test cleanup", now()
        )).isTrue();
        ClaimedScheduleFire claimedB = fireRepository.claimPending(
                tenantB, 1, "tenant-b-scheduler", now(), Duration.ofSeconds(30)
        ).getFirst();
        assertThat(fireRepository.markFailed(
                tenantB, claimedB.triggerId(), claimedB.claimToken(), "test cleanup", now()
        )).isTrue();
    }

    @Test
    void startsAOneTimeScheduleExactlyOnceAndCompletesIt() {
        WorkflowDefinition workflow = publishedWorkflow("One-time trigger");
        WorkflowSchedule schedule = oneTimeSchedule(workflow.id(), MisfirePolicy.FIRE_ONCE);
        Instant due = now().minusSeconds(1);
        makeOneTimeDue(schedule.id(), due);

        ScheduleFireRunResult first = fireService.runOnce(10, 10);
        ScheduleFireRunResult second = fireService.runOnce(10, 10);

        WorkflowSchedule completed = schedules.get(TenantId.LOCAL, schedule.id());
        assertThat(first.materialized()).isEqualTo(1);
        assertThat(triggerErrors(schedule.id())).containsOnlyNulls();
        assertThat(first.started()).isEqualTo(1);
        assertThat(second.materialized()).isZero();
        assertThat(second.claimed()).isZero();
        assertThat(completed.status()).isEqualTo(ScheduleStatus.COMPLETED);
        assertThat(completed.nextFireAt()).isNull();
        assertThat(countExecutions(workflow.id())).isEqualTo(1);
        assertThat(triggerStatuses(schedule.id())).containsExactly("STARTED");
    }

    @Test
    void catchesUpOnceOrSkipsAfterDowntimeAccordingToMisfirePolicy() {
        WorkflowDefinition workflow = publishedWorkflow("Misfire policy");
        WorkflowSchedule catchUp = cronSchedule(workflow.id(), MisfirePolicy.FIRE_ONCE);
        WorkflowSchedule skip = cronSchedule(workflow.id(), MisfirePolicy.SKIP);
        Instant staleFire = now().minus(Duration.ofMinutes(10));
        makeCronDue(catchUp.id(), staleFire);
        makeCronDue(skip.id(), staleFire);

        ScheduleFireRunResult result = fireService.runOnce(10, 10);

        assertThat(result.materialized()).isEqualTo(2);
        assertThat(result.skipped()).isEqualTo(1);
        assertThat(triggerErrors(catchUp.id())).containsOnlyNulls();
        assertThat(result.started()).isEqualTo(1);
        assertThat(triggerStatuses(catchUp.id())).containsExactly("STARTED");
        assertThat(triggerStatuses(skip.id())).containsExactly("SKIPPED");
        assertThat(schedules.get(TenantId.LOCAL, catchUp.id()).nextFireAt()).isAfter(Instant.now());
        assertThat(schedules.get(TenantId.LOCAL, skip.id()).nextFireAt()).isAfter(Instant.now());
        assertThat(countExecutions(workflow.id())).isEqualTo(1);
    }

    @Test
    void competingSchedulersMaterializeAndClaimDisjointBatches() throws Exception {
        jdbc.sql("DELETE FROM workflow_schedule_trigger WHERE status IN ('PENDING', 'PROCESSING')")
                .update();
        jdbc.sql("UPDATE workflow_schedule SET status = 'PAUSED' WHERE status = 'ACTIVE'")
                .update();
        WorkflowDefinition workflow = publishedWorkflow("Competing schedulers");
        Instant due = now().minusSeconds(1);
        for (int index = 0; index < 12; index++) {
            WorkflowSchedule schedule = oneTimeSchedule(workflow.id(), MisfirePolicy.FIRE_ONCE);
            makeOneTimeDue(schedule.id(), due.plusNanos(index * 1_000L));
        }

        CountDownLatch start = new CountDownLatch(1);
        ScheduleMaterializationResult left;
        ScheduleMaterializationResult right;
        try (var pool = Executors.newFixedThreadPool(2)) {
            var leftFuture = pool.submit(() -> {
                start.await();
                return fireRepository.materializeDue(6, now(), Duration.ofMinutes(1));
            });
            var rightFuture = pool.submit(() -> {
                start.await();
                return fireRepository.materializeDue(6, now(), Duration.ofMinutes(1));
            });
            start.countDown();
            left = leftFuture.get(10, TimeUnit.SECONDS);
            right = rightFuture.get(10, TimeUnit.SECONDS);
        }

        Instant claimTime = now().plusSeconds(1);
        List<ClaimedScheduleFire> firstClaims;
        List<ClaimedScheduleFire> secondClaims;
        try (var pool = Executors.newFixedThreadPool(2)) {
            var first = pool.submit(() -> fireRepository.claimPending(
                    TenantId.LOCAL, 6, "scheduler-a", claimTime, Duration.ofSeconds(30)
            ));
            var second = pool.submit(() -> fireRepository.claimPending(
                    TenantId.LOCAL, 6, "scheduler-b", claimTime, Duration.ofSeconds(30)
            ));
            firstClaims = first.get(10, TimeUnit.SECONDS);
            secondClaims = second.get(10, TimeUnit.SECONDS);
        }

        Set<UUID> firstIds = triggerIds(firstClaims);
        Set<UUID> secondIds = triggerIds(secondClaims);
        Set<UUID> allIds = new HashSet<>(firstIds);
        allIds.addAll(secondIds);
        assertThat(left.due() + right.due()).isEqualTo(12);
        assertThat(countTriggersForWorkflow(workflow.id())).isEqualTo(12);
        assertThat(firstClaims).hasSize(6);
        assertThat(secondClaims).hasSize(6);
        assertThat(firstIds).doesNotContainAnyElementsOf(secondIds);
        assertThat(allIds).hasSize(12);
    }

    @Test
    void anExpiredClaimIsRecoveredAndTheOldTokenIsFenced() {
        WorkflowDefinition workflow = publishedWorkflow("Lease recovery");
        WorkflowSchedule schedule = oneTimeSchedule(workflow.id(), MisfirePolicy.FIRE_ONCE);
        Instant due = now().minusSeconds(1);
        makeOneTimeDue(schedule.id(), due);
        fireRepository.materializeDue(1, now(), Duration.ofMinutes(1));

        Instant claimedAt = now();
        ClaimedScheduleFire abandoned = fireRepository.claimPending(
                TenantId.LOCAL, 1, "failed-scheduler", claimedAt, Duration.ofSeconds(1)
        ).getFirst();
        assertThat(fireRepository.claimPending(
                TenantId.LOCAL, 1, "early-replacement", claimedAt.plusMillis(999), Duration.ofSeconds(1)
        )).isEmpty();

        ClaimedScheduleFire replacement = fireRepository.claimPending(
                TenantId.LOCAL, 1, "replacement", claimedAt.plusSeconds(1), Duration.ofSeconds(30)
        ).getFirst();
        assertThat(fireRepository.markStarted(
                TenantId.LOCAL, abandoned.triggerId(), abandoned.claimToken(),
                UUID.randomUUID(), claimedAt.plusSeconds(2)
        )).isFalse();
        assertThat(fireRepository.markFailed(
                TenantId.LOCAL, replacement.triggerId(), replacement.claimToken(),
                "replacement-test", claimedAt.plusSeconds(2)
        )).isTrue();
        assertThat(replacement.attemptCount()).isEqualTo(2);
        assertThat(countExecutions(workflow.id())).isZero();
        assertThat(triggerStatuses(schedule.id())).containsExactly("FAILED");
    }

    @Test
    void boundsPendingMaterializationAcrossTheDurableQueue() {
        WorkflowDefinition workflow = publishedWorkflow("Pending queue bound");
        WorkflowSchedule first = oneTimeSchedule(workflow.id(), MisfirePolicy.FIRE_ONCE);
        WorkflowSchedule second = oneTimeSchedule(workflow.id(), MisfirePolicy.FIRE_ONCE);
        Instant due = now().minusSeconds(1);
        makeOneTimeDue(first.id(), due);
        makeOneTimeDue(second.id(), due.plusNanos(1_000));
        QueueSnapshot before = fireRepository.pendingQueue(TenantId.LOCAL, now());

        ScheduleMaterializationResult result = fireRepository.materializeDue(
                10, Math.toIntExact(before.depth() + 1), now(), Duration.ofMinutes(1)
        );

        assertThat(result.pending()).isEqualTo(1);
        assertThat(result.capacityDeferred()).isEqualTo(1);
        assertThat(countTriggersForWorkflow(workflow.id())).isEqualTo(1);
        assertThat(fireRepository.pendingQueue(TenantId.LOCAL, now()).depth())
                .isEqualTo(before.depth() + 1);

        jdbc.sql("DELETE FROM workflow_schedule_trigger WHERE workflow_id = :workflowId")
                .param("workflowId", workflow.id())
                .update();
        jdbc.sql("""
                UPDATE workflow_schedule
                   SET status = 'DELETED', next_fire_at = COALESCE(next_fire_at, :now)
                 WHERE workflow_id = :workflowId
                """)
                .param("workflowId", workflow.id())
                .param("now", Timestamp.from(now()))
                .update();
    }

    private WorkflowSchedule oneTimeSchedule(UUID workflowId, MisfirePolicy policy) {
        return oneTimeSchedule(TenantId.LOCAL, workflowId, policy);
    }

    private WorkflowSchedule oneTimeSchedule(
            TenantId tenantId,
            UUID workflowId,
            MisfirePolicy policy
    ) {
        return schedules.create(tenantId, new WorkflowScheduleDraft(
                workflowId,
                new OneTimeSchedule(now().plus(Duration.ofHours(1))),
                policy
        ));
    }

    private WorkflowSchedule cronSchedule(UUID workflowId, MisfirePolicy policy) {
        return schedules.create(TenantId.LOCAL, new WorkflowScheduleDraft(
                workflowId,
                new CronSchedule("0 * * * * *", ZoneId.of("UTC")),
                policy
        ));
    }

    private void makeOneTimeDue(UUID scheduleId, Instant due) {
        jdbc.sql("""
                UPDATE workflow_schedule
                   SET one_time_at = :due, next_fire_at = :due
                 WHERE id = :id
                """)
                .param("due", Timestamp.from(due))
                .param("id", scheduleId)
                .update();
    }

    private void makeCronDue(UUID scheduleId, Instant due) {
        jdbc.sql("UPDATE workflow_schedule SET next_fire_at = :due WHERE id = :id")
                .param("due", Timestamp.from(due))
                .param("id", scheduleId)
                .update();
    }

    private long countExecutions(UUID workflowId) {
        return jdbc.sql("SELECT COUNT(*) FROM workflow_execution WHERE workflow_id = :workflowId")
                .param("workflowId", workflowId)
                .query(Long.class)
                .single();
    }

    private long countTriggersForWorkflow(UUID workflowId) {
        return jdbc.sql("SELECT COUNT(*) FROM workflow_schedule_trigger WHERE workflow_id = :workflowId")
                .param("workflowId", workflowId)
                .query(Long.class)
                .single();
    }

    private List<String> triggerStatuses(UUID scheduleId) {
        return jdbc.sql("""
                SELECT status
                  FROM workflow_schedule_trigger
                 WHERE schedule_id = :scheduleId
                 ORDER BY scheduled_fire_at
                """)
                .param("scheduleId", scheduleId)
                .query(String.class)
                .list();
    }

    private List<String> triggerErrors(UUID scheduleId) {
        return jdbc.sql("""
                SELECT error_message
                  FROM workflow_schedule_trigger
                 WHERE schedule_id = :scheduleId
                 ORDER BY scheduled_fire_at
                """)
                .param("scheduleId", scheduleId)
                .query(String.class)
                .list();
    }

    private WorkflowDefinition publishedWorkflow(String name) {
        return publishedWorkflow(TenantId.LOCAL, name);
    }

    private WorkflowDefinition publishedWorkflow(TenantId tenantId, String name) {
        WorkflowDefinition draft = workflows.create(tenantId, new WorkflowDraft(
                name,
                null,
                List.of(new TaskDefinition("ROOT", "Root", "NOOP", Map.of())),
                List.of()
        ));
        return workflows.publish(tenantId, draft.id(), draft.lockVersion());
    }

    private TenantId registerTenant(String tenantId, String displayName) {
        jdbc.sql("""
                INSERT INTO tenant_registry(tenant_id, display_name, status)
                VALUES (:tenantId, :displayName, 'ACTIVE')
                ON CONFLICT (tenant_id) DO NOTHING
                """)
                .param("tenantId", tenantId)
                .param("displayName", displayName)
                .update();
        return new TenantId(tenantId);
    }

    private static TenantQuotaPolicy quotaPolicy(int maxPending) {
        TokenBucketPolicy rate = new TokenBucketPolicy(100, 100, Duration.ofSeconds(1));
        return new TenantQuotaPolicy(100, 100, maxPending, 100, rate, rate);
    }

    private static Set<UUID> triggerIds(List<ClaimedScheduleFire> fires) {
        Set<UUID> ids = new HashSet<>();
        fires.forEach(fire -> ids.add(fire.triggerId()));
        return ids;
    }

    private static Instant now() {
        return Instant.now().truncatedTo(ChronoUnit.MICROS);
    }
}
