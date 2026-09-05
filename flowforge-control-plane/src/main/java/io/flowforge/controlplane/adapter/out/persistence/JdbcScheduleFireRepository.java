package io.flowforge.controlplane.adapter.out.persistence;

import io.flowforge.application.schedule.ClaimedScheduleFire;
import io.flowforge.application.schedule.ScheduleCalculator;
import io.flowforge.application.schedule.ScheduleFireRepository;
import io.flowforge.application.schedule.ScheduleFireService;
import io.flowforge.application.schedule.ScheduleMaterializationResult;
import io.flowforge.application.schedule.QueueSnapshot;
import io.flowforge.domain.schedule.CronSchedule;
import io.flowforge.domain.schedule.MisfirePolicy;
import io.flowforge.domain.schedule.OneTimeSchedule;
import io.flowforge.domain.schedule.ScheduleSpec;
import io.flowforge.domain.schedule.ScheduleTriggerStatus;
import io.flowforge.domain.schedule.ScheduleType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

@Repository
public class JdbcScheduleFireRepository implements ScheduleFireRepository {
    private final JdbcClient jdbc;
    private final ScheduleCalculator calculator;

    public JdbcScheduleFireRepository(JdbcClient jdbc, ScheduleCalculator calculator) {
        this.jdbc = jdbc;
        this.calculator = calculator;
    }

    @Override
    @Transactional
    public ScheduleMaterializationResult materializeDue(
            int limit,
            Instant now,
            Duration misfireThreshold
    ) {
        return materializeDue(limit, Integer.MAX_VALUE, now, misfireThreshold);
    }

    @Override
    @Transactional
    public ScheduleMaterializationResult materializeDue(
            int limit,
            int maxPending,
            Instant now,
            Duration misfireThreshold
    ) {
        lockCapacity("backpressure:schedule-pending");
        long pendingCount = pendingCount();
        int availableCapacity = (int) Math.max(0, Math.min(limit, maxPending - pendingCount));
        List<DueSchedule> due = jdbc.sql("""
                SELECT id, workflow_id, schedule_type, one_time_at, cron_expression, time_zone,
                       misfire_policy, next_fire_at
                  FROM workflow_schedule
                 WHERE status = 'ACTIVE'
                   AND next_fire_at <= :now
                 ORDER BY next_fire_at, id
                 FOR UPDATE SKIP LOCKED
                 LIMIT :limit
                """)
                .param("now", timestamp(now))
                .param("limit", availableCapacity)
                .query(this::mapDueSchedule)
                .list();

        int pending = 0;
        int skipped = 0;
        for (DueSchedule schedule : due) {
            boolean isMisfire = schedule.scheduledFireAt().plus(misfireThreshold).isBefore(now);
            boolean shouldSkip = schedule.misfirePolicy() == MisfirePolicy.SKIP && isMisfire;
            ScheduleTriggerStatus triggerStatus = shouldSkip
                    ? ScheduleTriggerStatus.SKIPPED
                    : ScheduleTriggerStatus.PENDING;
            insertTrigger(schedule, triggerStatus, now);
            advance(schedule, now);
            if (shouldSkip) skipped++; else pending++;
        }
        int capacityDeferred = pendingCount + pending >= maxPending && hasDueSchedule(now) ? 1 : 0;
        return new ScheduleMaterializationResult(due.size(), pending, skipped, capacityDeferred);
    }

    @Override
    @Transactional(readOnly = true)
    public QueueSnapshot pendingQueue(Instant now) {
        return jdbc.sql("""
                SELECT COUNT(*) AS depth, MIN(created_at) AS oldest
                  FROM workflow_schedule_trigger
                 WHERE status IN ('PENDING', 'PROCESSING')
                """)
                .query((rs, rowNumber) -> {
                    long depth = rs.getLong("depth");
                    Instant oldest = instant(rs.getObject("oldest"));
                    Duration age = oldest == null || oldest.isAfter(now)
                            ? Duration.ZERO
                            : Duration.between(oldest, now);
                    return new QueueSnapshot(depth, age);
                })
                .single();
    }

    @Override
    @Transactional
    public List<ClaimedScheduleFire> claimPending(
            int limit,
            String claimant,
            Instant now,
            Duration leaseDuration
    ) {
        if (limit == 0) return List.of();
        UUID claimToken = UUID.randomUUID();
        return jdbc.sql("""
                WITH candidates AS (
                    SELECT id
                      FROM workflow_schedule_trigger
                     WHERE (status = 'PENDING' AND available_at <= :now)
                        OR (status = 'PROCESSING' AND claimed_until <= :now)
                     ORDER BY available_at, scheduled_fire_at, id
                     FOR UPDATE SKIP LOCKED
                     LIMIT :limit
                )
                UPDATE workflow_schedule_trigger target
                   SET status = 'PROCESSING',
                       attempt_count = target.attempt_count + 1,
                       claimed_by = :claimant,
                       claimed_at = :now,
                       claimed_until = :claimedUntil,
                       claim_token = :claimToken
                  FROM candidates
                 WHERE target.id = candidates.id
                RETURNING target.id, target.schedule_id, target.workflow_id,
                          target.scheduled_fire_at, target.idempotency_key,
                          target.attempt_count, target.claim_token
                """)
                .param("now", timestamp(now))
                .param("limit", limit)
                .param("claimant", truncate(claimant, 200))
                .param("claimedUntil", timestamp(now.plus(leaseDuration)))
                .param("claimToken", claimToken)
                .query((rs, rowNumber) -> new ClaimedScheduleFire(
                        rs.getObject("id", UUID.class),
                        rs.getObject("schedule_id", UUID.class),
                        rs.getObject("workflow_id", UUID.class),
                        instant(rs.getObject("scheduled_fire_at")),
                        rs.getString("idempotency_key"),
                        rs.getInt("attempt_count"),
                        rs.getObject("claim_token", UUID.class)
                ))
                .list();
    }

    private void lockCapacity(String resourceKey) {
        jdbc.sql("""
                SELECT pg_advisory_xact_lock(hashtextextended(:resourceKey, 0))
                """)
                .param("resourceKey", resourceKey)
                .query((rs, rowNumber) -> 1)
                .single();
    }

    private long pendingCount() {
        return jdbc.sql("""
                SELECT COUNT(*) FROM workflow_schedule_trigger
                 WHERE status IN ('PENDING', 'PROCESSING')
                """)
                .query(Long.class)
                .single();
    }

    private boolean hasDueSchedule(Instant now) {
        return jdbc.sql("""
                SELECT EXISTS(
                    SELECT 1 FROM workflow_schedule
                     WHERE status = 'ACTIVE' AND next_fire_at <= :now
                )
                """)
                .param("now", timestamp(now))
                .query(Boolean.class)
                .single();
    }

    @Override
    @Transactional
    public boolean markStarted(
            UUID triggerId,
            UUID claimToken,
            UUID workflowExecutionId,
            Instant now
    ) {
        return complete(
                triggerId,
                claimToken,
                ScheduleTriggerStatus.STARTED,
                workflowExecutionId,
                null,
                now
        );
    }

    @Override
    @Transactional
    public boolean markFailed(UUID triggerId, UUID claimToken, String errorMessage, Instant now) {
        return complete(
                triggerId,
                claimToken,
                ScheduleTriggerStatus.FAILED,
                null,
                errorMessage,
                now
        );
    }

    @Override
    @Transactional
    public boolean release(
            UUID triggerId,
            UUID claimToken,
            String errorMessage,
            Instant availableAt
    ) {
        int updated = jdbc.sql("""
                UPDATE workflow_schedule_trigger
                   SET status = 'PENDING',
                       available_at = :availableAt,
                       claimed_by = NULL,
                       claimed_at = NULL,
                       claimed_until = NULL,
                       claim_token = NULL,
                       error_message = :errorMessage
                 WHERE id = :id
                   AND status = 'PROCESSING'
                   AND claim_token = :claimToken
                """)
                .param("availableAt", timestamp(availableAt))
                .param("errorMessage", truncate(errorMessage, 2_000))
                .param("id", triggerId)
                .param("claimToken", claimToken)
                .update();
        return updated == 1;
    }

    private void insertTrigger(DueSchedule schedule, ScheduleTriggerStatus status, Instant now) {
        jdbc.sql("""
                INSERT INTO workflow_schedule_trigger(
                    id, schedule_id, workflow_id, scheduled_fire_at, idempotency_key,
                    status, available_at, created_at, processed_at
                ) VALUES (
                    :id, :scheduleId, :workflowId, :scheduledFireAt, :idempotencyKey,
                    :status, :availableAt, :createdAt, :processedAt
                )
                ON CONFLICT (schedule_id, scheduled_fire_at) DO NOTHING
                """)
                .param("id", UUID.randomUUID())
                .param("scheduleId", schedule.id())
                .param("workflowId", schedule.workflowId())
                .param("scheduledFireAt", timestamp(schedule.scheduledFireAt()))
                .param("idempotencyKey", ScheduleFireService.idempotencyKey(
                        schedule.id(), schedule.scheduledFireAt()
                ))
                .param("status", status.name())
                .param("availableAt", timestamp(now))
                .param("createdAt", timestamp(now))
                .param("processedAt", status == ScheduleTriggerStatus.SKIPPED ? timestamp(now) : null)
                .update();
    }

    private void advance(DueSchedule schedule, Instant now) {
        Instant nextFireAt = schedule.spec() instanceof CronSchedule
                ? calculator.nextFireAt(schedule.spec(), now)
                : null;
        String status = nextFireAt == null ? "COMPLETED" : "ACTIVE";
        jdbc.sql("""
                UPDATE workflow_schedule
                   SET status = :status,
                       next_fire_at = :nextFireAt,
                       lock_version = lock_version + 1,
                       updated_at = :now
                 WHERE id = :id
                """)
                .param("status", status)
                .param("nextFireAt", timestamp(nextFireAt))
                .param("now", timestamp(now))
                .param("id", schedule.id())
                .update();
    }

    private boolean complete(
            UUID triggerId,
            UUID claimToken,
            ScheduleTriggerStatus status,
            UUID workflowExecutionId,
            String errorMessage,
            Instant now
    ) {
        int updated = jdbc.sql("""
                UPDATE workflow_schedule_trigger
                   SET status = :status,
                       workflow_execution_id = :workflowExecutionId,
                       error_message = :errorMessage,
                       processed_at = :now,
                       claimed_by = NULL,
                       claimed_at = NULL,
                       claimed_until = NULL,
                       claim_token = NULL
                 WHERE id = :id
                   AND status = 'PROCESSING'
                   AND claim_token = :claimToken
                """)
                .param("status", status.name())
                .param("workflowExecutionId", workflowExecutionId)
                .param("errorMessage", truncate(errorMessage, 2_000))
                .param("now", timestamp(now))
                .param("id", triggerId)
                .param("claimToken", claimToken)
                .update();
        return updated == 1;
    }

    private DueSchedule mapDueSchedule(ResultSet rs, int rowNumber) throws SQLException {
        ScheduleType type = ScheduleType.valueOf(rs.getString("schedule_type"));
        ScheduleSpec spec = type == ScheduleType.ONE_TIME
                ? new OneTimeSchedule(instant(rs.getObject("one_time_at")))
                : new CronSchedule(rs.getString("cron_expression"), ZoneId.of(rs.getString("time_zone")));
        return new DueSchedule(
                rs.getObject("id", UUID.class),
                rs.getObject("workflow_id", UUID.class),
                spec,
                MisfirePolicy.valueOf(rs.getString("misfire_policy")),
                instant(rs.getObject("next_fire_at"))
        );
    }

    private static String truncate(String value, int maximumLength) {
        if (value == null || value.length() <= maximumLength) return value;
        return value.substring(0, maximumLength);
    }

    private static Timestamp timestamp(Instant value) {
        return value == null ? null : Timestamp.from(value);
    }

    private static Instant instant(Object value) {
        if (value == null) return null;
        if (value instanceof OffsetDateTime timestamp) return timestamp.toInstant();
        if (value instanceof Timestamp timestamp) return timestamp.toInstant();
        if (value instanceof Instant instant) return instant;
        throw new IllegalStateException("Unsupported timestamp value: " + value);
    }

    private record DueSchedule(
            UUID id,
            UUID workflowId,
            ScheduleSpec spec,
            MisfirePolicy misfirePolicy,
            Instant scheduledFireAt
    ) {
    }
}
