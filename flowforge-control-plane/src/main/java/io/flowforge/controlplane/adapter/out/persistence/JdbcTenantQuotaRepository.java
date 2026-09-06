package io.flowforge.controlplane.adapter.out.persistence;

import io.flowforge.application.coordination.TokenBucketPolicy;
import io.flowforge.application.tenancy.TenantQuota;
import io.flowforge.application.tenancy.TenantQuotaConflictException;
import io.flowforge.application.tenancy.TenantQuotaPolicy;
import io.flowforge.application.tenancy.TenantQuotaProvider;
import io.flowforge.application.tenancy.TenantQuotaRepository;
import io.flowforge.domain.tenancy.TenantId;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;

@Repository
public class JdbcTenantQuotaRepository implements TenantQuotaRepository, TenantQuotaProvider {
    private final JdbcClient jdbc;
    private final TenantQuotaPolicy defaults;

    public JdbcTenantQuotaRepository(JdbcClient jdbc, TenantQuotaPolicy defaults) {
        this.jdbc = jdbc;
        this.defaults = defaults;
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<TenantQuota> find(TenantId tenantId) {
        return jdbc.sql("""
                SELECT tenant_id, quota_version, max_active_executions, max_running_tasks,
                       max_pending_schedule_fires, max_ready_tasks,
                       schedule_rate_capacity, schedule_rate_refill_tokens,
                       schedule_rate_refill_period_ms, dispatch_rate_capacity,
                       dispatch_rate_refill_tokens, dispatch_rate_refill_period_ms,
                       created_at, updated_at
                  FROM tenant_quota
                 WHERE tenant_id = :tenantId
                """)
                .param("tenantId", tenantId.value())
                .query(this::map)
                .optional();
    }

    @Override
    public TenantQuota quotaFor(TenantId tenantId) {
        return find(tenantId).orElseGet(() -> TenantQuota.inherited(tenantId, defaults));
    }

    @Override
    @Transactional
    public TenantQuota save(
            TenantId tenantId,
            TenantQuotaPolicy policy,
            long expectedVersion,
            Instant now
    ) {
        int changed = expectedVersion == 0
                ? insert(tenantId, policy, now)
                : update(tenantId, policy, expectedVersion, now);
        if (changed != 1) throw new TenantQuotaConflictException(tenantId, expectedVersion);
        return find(tenantId).orElseThrow();
    }

    private int insert(TenantId tenantId, TenantQuotaPolicy policy, Instant now) {
        return parameters(jdbc.sql("""
                INSERT INTO tenant_quota(
                    tenant_id, quota_version, max_active_executions, max_running_tasks,
                    max_pending_schedule_fires, max_ready_tasks,
                    schedule_rate_capacity, schedule_rate_refill_tokens,
                    schedule_rate_refill_period_ms, dispatch_rate_capacity,
                    dispatch_rate_refill_tokens, dispatch_rate_refill_period_ms,
                    created_at, updated_at
                ) VALUES (
                    :tenantId, 1, :maxActiveExecutions, :maxRunningTasks,
                    :maxPendingScheduleFires, :maxReadyTasks,
                    :scheduleCapacity, :scheduleRefillTokens, :scheduleRefillPeriodMs,
                    :dispatchCapacity, :dispatchRefillTokens, :dispatchRefillPeriodMs,
                    :now, :now
                )
                ON CONFLICT (tenant_id) DO NOTHING
                """), tenantId, policy, now).update();
    }

    private int update(TenantId tenantId, TenantQuotaPolicy policy, long expectedVersion, Instant now) {
        return parameters(jdbc.sql("""
                UPDATE tenant_quota
                   SET quota_version = quota_version + 1,
                       max_active_executions = :maxActiveExecutions,
                       max_running_tasks = :maxRunningTasks,
                       max_pending_schedule_fires = :maxPendingScheduleFires,
                       max_ready_tasks = :maxReadyTasks,
                       schedule_rate_capacity = :scheduleCapacity,
                       schedule_rate_refill_tokens = :scheduleRefillTokens,
                       schedule_rate_refill_period_ms = :scheduleRefillPeriodMs,
                       dispatch_rate_capacity = :dispatchCapacity,
                       dispatch_rate_refill_tokens = :dispatchRefillTokens,
                       dispatch_rate_refill_period_ms = :dispatchRefillPeriodMs,
                       updated_at = GREATEST(updated_at, :now)
                 WHERE tenant_id = :tenantId AND quota_version = :expectedVersion
                """)
                .param("expectedVersion", expectedVersion), tenantId, policy, now).update();
    }

    private static JdbcClient.StatementSpec parameters(
            JdbcClient.StatementSpec statement,
            TenantId tenantId,
            TenantQuotaPolicy policy,
            Instant now
    ) {
        return statement
                .param("tenantId", tenantId.value())
                .param("maxActiveExecutions", policy.maxActiveExecutions())
                .param("maxRunningTasks", policy.maxRunningTasks())
                .param("maxPendingScheduleFires", policy.maxPendingScheduleFires())
                .param("maxReadyTasks", policy.maxReadyTasks())
                .param("scheduleCapacity", policy.scheduleRateLimit().capacity())
                .param("scheduleRefillTokens", policy.scheduleRateLimit().refillTokens())
                .param("scheduleRefillPeriodMs", policy.scheduleRateLimit().refillPeriod().toMillis())
                .param("dispatchCapacity", policy.dispatchRateLimit().capacity())
                .param("dispatchRefillTokens", policy.dispatchRateLimit().refillTokens())
                .param("dispatchRefillPeriodMs", policy.dispatchRateLimit().refillPeriod().toMillis())
                .param("now", Timestamp.from(now));
    }

    private TenantQuota map(ResultSet rs, int rowNumber) throws SQLException {
        TenantQuotaPolicy policy = new TenantQuotaPolicy(
                rs.getInt("max_active_executions"),
                rs.getInt("max_running_tasks"),
                rs.getInt("max_pending_schedule_fires"),
                rs.getInt("max_ready_tasks"),
                new TokenBucketPolicy(
                        rs.getInt("schedule_rate_capacity"),
                        rs.getInt("schedule_rate_refill_tokens"),
                        Duration.ofMillis(rs.getLong("schedule_rate_refill_period_ms"))
                ),
                new TokenBucketPolicy(
                        rs.getInt("dispatch_rate_capacity"),
                        rs.getInt("dispatch_rate_refill_tokens"),
                        Duration.ofMillis(rs.getLong("dispatch_rate_refill_period_ms"))
                )
        );
        return new TenantQuota(
                new TenantId(rs.getString("tenant_id")),
                rs.getLong("quota_version"),
                policy,
                true,
                instant(rs.getObject("created_at")),
                instant(rs.getObject("updated_at"))
        );
    }

    private static Instant instant(Object value) {
        if (value instanceof OffsetDateTime timestamp) return timestamp.toInstant();
        if (value instanceof Timestamp timestamp) return timestamp.toInstant();
        if (value instanceof Instant instant) return instant;
        throw new IllegalStateException("Unsupported timestamp value: " + value);
    }
}
