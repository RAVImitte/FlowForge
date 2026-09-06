package io.flowforge.controlplane.adapter.out.persistence;

import io.flowforge.application.schedule.ScheduleConflictException;
import io.flowforge.application.schedule.ScheduleNotFoundException;
import io.flowforge.application.schedule.ScheduleRepository;
import io.flowforge.application.workflow.PageResult;
import io.flowforge.domain.schedule.CronSchedule;
import io.flowforge.domain.schedule.MisfirePolicy;
import io.flowforge.domain.schedule.OneTimeSchedule;
import io.flowforge.domain.schedule.ScheduleSpec;
import io.flowforge.domain.schedule.ScheduleStatus;
import io.flowforge.domain.schedule.ScheduleType;
import io.flowforge.domain.schedule.WorkflowSchedule;
import io.flowforge.domain.schedule.WorkflowScheduleDraft;
import io.flowforge.domain.tenancy.TenantId;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcScheduleRepository implements ScheduleRepository {
    private static final String SELECT_ACTIVE = """
            SELECT id, tenant_id, workflow_id, schedule_type, one_time_at, cron_expression, time_zone,
                   misfire_policy, status, next_fire_at, lock_version, created_at, updated_at
              FROM workflow_schedule
             WHERE tenant_id = :tenantId AND id = :id AND status <> 'DELETED'
            """;

    private final JdbcClient jdbc;

    public JdbcScheduleRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    @Transactional
    public WorkflowSchedule create(
            TenantId tenantId,
            WorkflowScheduleDraft draft,
            Instant nextFireAt,
            Instant now
    ) {
        UUID id = UUID.randomUUID();
        writeInsert(tenantId, id, draft, nextFireAt, now);
        return findById(tenantId, id).orElseThrow();
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<WorkflowSchedule> findById(TenantId tenantId, UUID id) {
        return jdbc.sql(SELECT_ACTIVE)
                .param("tenantId", tenantId.value())
                .param("id", id)
                .query(this::map)
                .optional();
    }

    @Override
    @Transactional(readOnly = true)
    public PageResult<WorkflowSchedule> findAll(TenantId tenantId, int page, int size) {
        long total = jdbc.sql("""
                SELECT COUNT(*) FROM workflow_schedule
                 WHERE tenant_id = :tenantId AND status <> 'DELETED'
                """)
                .param("tenantId", tenantId.value())
                .query(Long.class).single();
        List<WorkflowSchedule> items = jdbc.sql("""
                SELECT id, tenant_id, workflow_id, schedule_type, one_time_at, cron_expression, time_zone,
                       misfire_policy, status, next_fire_at, lock_version, created_at, updated_at
                  FROM workflow_schedule
                 WHERE tenant_id = :tenantId AND status <> 'DELETED'
                 ORDER BY created_at DESC, id
                 LIMIT :limit OFFSET :offset
                """)
                .param("tenantId", tenantId.value())
                .param("limit", size)
                .param("offset", page * size)
                .query(this::map)
                .list();
        return new PageResult<>(items, page, size, total);
    }

    @Override
    @Transactional
    public WorkflowSchedule update(
            TenantId tenantId,
            UUID id,
            long expectedLockVersion,
            WorkflowScheduleDraft draft,
            Instant nextFireAt,
            Instant now
    ) {
        ensureMutable(id, lockAndCheck(tenantId, id, expectedLockVersion));
        ScheduleColumns columns = columns(draft.spec());
        jdbc.sql("""
                UPDATE workflow_schedule
                   SET workflow_id = :workflowId,
                       schedule_type = :scheduleType,
                       one_time_at = :oneTimeAt,
                       cron_expression = :cronExpression,
                       time_zone = :timeZone,
                       misfire_policy = :misfirePolicy,
                       next_fire_at = :nextFireAt,
                       lock_version = lock_version + 1,
                       updated_at = :now
                 WHERE tenant_id = :tenantId AND id = :id
                """)
                .param("workflowId", draft.workflowId())
                .param("scheduleType", draft.spec().type().name())
                .param("oneTimeAt", timestamp(columns.oneTimeAt()))
                .param("cronExpression", columns.cronExpression())
                .param("timeZone", columns.timeZone())
                .param("misfirePolicy", draft.misfirePolicy().name())
                .param("nextFireAt", timestamp(nextFireAt))
                .param("now", timestamp(now))
                .param("tenantId", tenantId.value())
                .param("id", id)
                .update();
        return findById(tenantId, id).orElseThrow();
    }

    @Override
    @Transactional
    public WorkflowSchedule changeStatus(
            TenantId tenantId,
            UUID id,
            long expectedLockVersion,
            ScheduleStatus status,
            Instant nextFireAt,
            Instant now
    ) {
        if (status != ScheduleStatus.ACTIVE && status != ScheduleStatus.PAUSED) {
            throw new IllegalArgumentException("Only ACTIVE and PAUSED are mutable statuses");
        }
        ensureMutable(id, lockAndCheck(tenantId, id, expectedLockVersion));
        jdbc.sql("""
                UPDATE workflow_schedule
                   SET status = :status,
                       next_fire_at = :nextFireAt,
                       lock_version = lock_version + 1,
                       updated_at = :now
                 WHERE tenant_id = :tenantId AND id = :id
                """)
                .param("status", status.name())
                .param("nextFireAt", timestamp(nextFireAt))
                .param("now", timestamp(now))
                .param("tenantId", tenantId.value())
                .param("id", id)
                .update();
        return findById(tenantId, id).orElseThrow();
    }

    @Override
    @Transactional
    public void delete(TenantId tenantId, UUID id, long expectedLockVersion, Instant now) {
        lockAndCheck(tenantId, id, expectedLockVersion);
        jdbc.sql("""
                UPDATE workflow_schedule
                   SET status = 'DELETED',
                       lock_version = lock_version + 1,
                       updated_at = :now
                 WHERE tenant_id = :tenantId AND id = :id
                """)
                .param("tenantId", tenantId.value())
                .param("now", timestamp(now))
                .param("id", id)
                .update();
    }

    private void writeInsert(
            TenantId tenantId,
            UUID id,
            WorkflowScheduleDraft draft,
            Instant nextFireAt,
            Instant now
    ) {
        ScheduleColumns columns = columns(draft.spec());
        jdbc.sql("""
                INSERT INTO workflow_schedule(
                    id, tenant_id, workflow_id, schedule_type, one_time_at, cron_expression, time_zone,
                    misfire_policy, status, next_fire_at, created_at, updated_at
                ) VALUES (
                    :id, :tenantId, :workflowId, :scheduleType, :oneTimeAt, :cronExpression, :timeZone,
                    :misfirePolicy, 'ACTIVE', :nextFireAt, :now, :now
                )
                """)
                .param("id", id)
                .param("tenantId", tenantId.value())
                .param("workflowId", draft.workflowId())
                .param("scheduleType", draft.spec().type().name())
                .param("oneTimeAt", timestamp(columns.oneTimeAt()))
                .param("cronExpression", columns.cronExpression())
                .param("timeZone", columns.timeZone())
                .param("misfirePolicy", draft.misfirePolicy().name())
                .param("nextFireAt", timestamp(nextFireAt))
                .param("now", timestamp(now))
                .update();
    }

    private ScheduleStatus lockAndCheck(TenantId tenantId, UUID id, long expectedLockVersion) {
        Optional<Map<String, Object>> row = jdbc.sql("""
                SELECT lock_version, status FROM workflow_schedule
                 WHERE tenant_id = :tenantId AND id = :id
                 FOR UPDATE
                """)
                .param("tenantId", tenantId.value())
                .param("id", id)
                .query((rs, rowNum) -> Map.<String, Object>of(
                        "lockVersion", rs.getLong("lock_version"),
                        "status", rs.getString("status")
                )).optional();
        if (row.isEmpty() || "DELETED".equals(row.get().get("status"))) {
            throw new ScheduleNotFoundException(id);
        }
        long actual = ((Number) row.get().get("lockVersion")).longValue();
        if (actual != expectedLockVersion) {
            throw new ScheduleConflictException(
                    "Schedule was modified concurrently; expected version "
                            + expectedLockVersion + " but found " + actual
            );
        }
        return ScheduleStatus.valueOf((String) row.get().get("status"));
    }

    private static void ensureMutable(UUID id, ScheduleStatus status) {
        if (status == ScheduleStatus.COMPLETED) {
            throw new ScheduleConflictException("Completed schedule " + id + " cannot be modified");
        }
    }

    private WorkflowSchedule map(ResultSet rs, int rowNumber) throws SQLException {
        ScheduleType type = ScheduleType.valueOf(rs.getString("schedule_type"));
        ScheduleSpec spec = type == ScheduleType.ONE_TIME
                ? new OneTimeSchedule(instant(rs.getObject("one_time_at")))
                : new CronSchedule(
                        rs.getString("cron_expression"),
                        ZoneId.of(rs.getString("time_zone"))
                );
        return new WorkflowSchedule(
                rs.getObject("id", UUID.class),
                new TenantId(rs.getString("tenant_id")),
                rs.getObject("workflow_id", UUID.class),
                spec,
                MisfirePolicy.valueOf(rs.getString("misfire_policy")),
                ScheduleStatus.valueOf(rs.getString("status")),
                instant(rs.getObject("next_fire_at")),
                rs.getLong("lock_version"),
                instant(rs.getObject("created_at")),
                instant(rs.getObject("updated_at"))
        );
    }

    private static ScheduleColumns columns(ScheduleSpec spec) {
        if (spec instanceof OneTimeSchedule oneTime) {
            return new ScheduleColumns(oneTime.fireAt(), null, null);
        }
        CronSchedule cron = (CronSchedule) spec;
        return new ScheduleColumns(null, cron.expression(), cron.timeZone().getId());
    }

    private static Timestamp timestamp(Instant value) {
        return value == null ? null : Timestamp.from(value);
    }

    private static Instant instant(Object value) {
        if (value == null) return null;
        if (value instanceof OffsetDateTime timestamp) return timestamp.toInstant();
        if (value instanceof Timestamp timestamp) return timestamp.toInstant();
        throw new IllegalStateException("Unsupported timestamp value: " + value);
    }

    private record ScheduleColumns(Instant oneTimeAt, String cronExpression, String timeZone) {
    }
}
