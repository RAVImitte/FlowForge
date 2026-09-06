package io.flowforge.controlplane.adapter.out.persistence;

import io.flowforge.application.coordination.TokenBucketDecision;
import io.flowforge.application.coordination.CoordinationObserver;
import io.flowforge.application.coordination.EphemeralTokenBucketStore;
import io.flowforge.application.coordination.TokenBucketPolicy;
import io.flowforge.application.coordination.TokenBucketRateLimiter;
import io.flowforge.application.coordination.TokenBucketSnapshot;
import io.flowforge.domain.tenancy.TenantId;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;

@Repository
public class JdbcTokenBucketRateLimiter implements TokenBucketRateLimiter {
    private final JdbcClient jdbc;
    private final ObjectProvider<EphemeralTokenBucketStore> ephemeralStore;
    private final CoordinationObserver coordinationObserver;
    private final Duration mirrorTtl;

    public JdbcTokenBucketRateLimiter(
            JdbcClient jdbc,
            ObjectProvider<EphemeralTokenBucketStore> ephemeralStore,
            CoordinationObserver coordinationObserver,
            @Value("${flowforge.rate-limits.mirror-ttl:10m}") Duration mirrorTtl
    ) {
        this.jdbc = jdbc;
        this.ephemeralStore = ephemeralStore;
        this.coordinationObserver = coordinationObserver;
        if (mirrorTtl == null || mirrorTtl.isZero() || mirrorTtl.isNegative()) {
            throw new IllegalArgumentException("Rate-limit mirror TTL must be positive");
        }
        this.mirrorTtl = mirrorTtl;
    }

    @Override
    @Transactional
    public TokenBucketDecision consume(
            TenantId tenantId,
            String bucketKey,
            TokenBucketPolicy policy,
            int requested,
            Instant now
    ) {
        String key = requiredKey(bucketKey);
        if (requested < 1 || requested > 1_000) {
            throw new IllegalArgumentException("requested tokens must be between 1 and 1000");
        }
        long periodMillis = policy.refillPeriod().toMillis();
        jdbc.sql("""
                INSERT INTO admission_rate_bucket(
                    tenant_id, bucket_key, capacity, refill_tokens, refill_period_ms,
                    available_tokens, last_refill_at, updated_at
                ) VALUES (
                    :tenantId, :key, :capacity, :refillTokens, :refillPeriodMs,
                    :capacity, :now, :now
                )
                ON CONFLICT (tenant_id, bucket_key) DO NOTHING
                """)
                .param("tenantId", tenantId.value())
                .param("key", key)
                .param("capacity", policy.capacity())
                .param("refillTokens", policy.refillTokens())
                .param("refillPeriodMs", periodMillis)
                .param("now", Timestamp.from(now))
                .update();

        Bucket bucket = jdbc.sql("""
                SELECT capacity, refill_tokens, refill_period_ms,
                       available_tokens, state_version,
                       GREATEST(last_refill_at, :now) AS effective_refill_at,
                       GREATEST(0, EXTRACT(EPOCH FROM (:now - last_refill_at)) * 1000) AS elapsed_ms
                 FROM admission_rate_bucket
                 WHERE tenant_id = :tenantId AND bucket_key = :key
                 FOR UPDATE
                """)
                .param("tenantId", tenantId.value())
                .param("key", key)
                .param("now", Timestamp.from(now))
                .query((rs, rowNumber) -> new Bucket(
                        rs.getInt("capacity"),
                        rs.getInt("refill_tokens"),
                        rs.getLong("refill_period_ms"),
                        rs.getDouble("available_tokens"),
                        rs.getDouble("elapsed_ms"),
                        rs.getLong("state_version"),
                        instant(rs.getObject("effective_refill_at"))
                ))
                .single();

        double refilled = Math.min(
                policy.capacity(),
                bucket.availableTokens()
                        + (bucket.elapsedMillis() * policy.refillTokens() / periodMillis)
        );
        int granted = Math.min(requested, (int) Math.floor(refilled + 1e-9));
        double remaining = Math.max(0, refilled - granted);
        long nextVersion = bucket.stateVersion() + 1;
        Duration retryAfter = granted == requested
                ? Duration.ZERO
                : retryAfter(remaining, policy);

        jdbc.sql("""
                UPDATE admission_rate_bucket
                   SET capacity = :capacity,
                       refill_tokens = :refillTokens,
                       refill_period_ms = :refillPeriodMs,
                       available_tokens = :availableTokens,
                       last_refill_at = GREATEST(last_refill_at, :now),
                       updated_at = GREATEST(updated_at, :now),
                       state_version = :stateVersion
                 WHERE tenant_id = :tenantId AND bucket_key = :key
                """)
                .param("tenantId", tenantId.value())
                .param("capacity", policy.capacity())
                .param("refillTokens", policy.refillTokens())
                .param("refillPeriodMs", periodMillis)
                .param("availableTokens", remaining)
                .param("now", Timestamp.from(now))
                .param("stateVersion", nextVersion)
                .param("key", key)
                .update();
        scheduleMirror(new TokenBucketSnapshot(
                tenantId, key, policy, remaining, bucket.effectiveRefillAt(), nextVersion
        ));
        return new TokenBucketDecision(granted, retryAfter);
    }

    private void scheduleMirror(TokenBucketSnapshot snapshot) {
        EphemeralTokenBucketStore store = ephemeralStore.getIfAvailable();
        if (store == null) return;
        Runnable mirror = () -> {
            try {
                store.replaceIfNewer(snapshot, mirrorTtl);
            } catch (RuntimeException failure) {
                coordinationObserver.degraded("rate-limit-mirror");
            }
        };
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            mirror.run();
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                mirror.run();
            }
        });
    }

    private static Duration retryAfter(double available, TokenBucketPolicy policy) {
        double missing = Math.max(0, 1 - available);
        long millis = (long) Math.ceil(
                missing * policy.refillPeriod().toMillis() / policy.refillTokens()
        );
        return Duration.ofMillis(Math.max(1, millis));
    }

    private static String requiredKey(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("bucketKey must not be blank");
        }
        String normalized = value.strip();
        if (normalized.length() > 300) {
            throw new IllegalArgumentException("bucketKey must not exceed 300 characters");
        }
        return normalized;
    }

    private static Instant instant(Object value) {
        if (value instanceof OffsetDateTime timestamp) return timestamp.toInstant();
        if (value instanceof Timestamp timestamp) return timestamp.toInstant();
        if (value instanceof Instant instant) return instant;
        throw new IllegalStateException("Unsupported timestamp value: " + value);
    }

    private record Bucket(
            int capacity,
            int refillTokens,
            long refillPeriodMillis,
            double availableTokens,
            double elapsedMillis,
            long stateVersion,
            Instant effectiveRefillAt
    ) { }
}
