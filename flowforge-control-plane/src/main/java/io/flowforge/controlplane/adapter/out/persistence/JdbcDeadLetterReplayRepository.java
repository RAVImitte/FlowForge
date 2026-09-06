package io.flowforge.controlplane.adapter.out.persistence;

import io.flowforge.application.recovery.DeadLetterRecord;
import io.flowforge.application.recovery.DeadLetterReplayClaim;
import io.flowforge.application.recovery.DeadLetterReplayConflictException;
import io.flowforge.application.recovery.DeadLetterReplayRepository;
import io.flowforge.domain.tenancy.TenantId;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcDeadLetterReplayRepository implements DeadLetterReplayRepository {
    private final JdbcClient jdbc;

    public JdbcDeadLetterReplayRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    @Transactional
    public DeadLetterReplayClaim claim(
            TenantId tenantId,
            UUID idempotencyKey,
            DeadLetterRecord record,
            String actor,
            String reason,
            Instant now,
            Duration leaseDuration
    ) {
        Optional<ReplayRow> byKey = findByKeyForUpdate(tenantId, idempotencyKey);
        if (byKey.isPresent()) return reclaim(byKey.get(), record, now, leaseDuration);

        int inserted = jdbc.sql("""
                        INSERT INTO dlq_replay_request (
                            tenant_id, idempotency_key, dlq_topic, dlq_partition, dlq_offset,
                            dlq_record_id, source_topic, requested_by, reason, status,
                            attempt_count, lease_expires_at, created_at, updated_at
                        ) VALUES (
                            :tenantId, :idempotencyKey, :topic, :partition, :offset,
                            :recordId, :sourceTopic, :actor, :reason, 'PUBLISHING',
                            1, :leaseExpiresAt, :now, :now
                        )
                        ON CONFLICT DO NOTHING
                        """)
                .param("tenantId", tenantId.value())
                .param("idempotencyKey", idempotencyKey)
                .param("topic", record.location().topic())
                .param("partition", record.location().partition())
                .param("offset", record.location().offset())
                .param("recordId", record.recordId())
                .param("sourceTopic", record.sourceTopic())
                .param("actor", actor)
                .param("reason", reason)
                .param("leaseExpiresAt", Timestamp.from(now.plus(leaseDuration)))
                .param("now", Timestamp.from(now))
                .update();
        if (inserted == 1) {
            return new DeadLetterReplayClaim(idempotencyKey, DeadLetterReplayClaim.State.ACQUIRED, null);
        }

        ReplayRow concurrent = findByKeyForUpdate(tenantId, idempotencyKey)
                .or(() -> findByLocationForUpdate(tenantId, record))
                .orElseThrow(() -> new IllegalStateException("Conflicting replay claim disappeared"));
        return reclaim(concurrent, record, now, leaseDuration);
    }

    @Override
    @Transactional
    public void markPublished(TenantId tenantId, UUID idempotencyKey, Instant publishedAt) {
        int updated = jdbc.sql("""
                        UPDATE dlq_replay_request
                           SET status = 'PUBLISHED', lease_expires_at = NULL,
                               failure_class = NULL, published_at = :publishedAt, updated_at = :publishedAt
                         WHERE tenant_id = :tenantId
                           AND idempotency_key = :idempotencyKey
                           AND status = 'PUBLISHING'
                        """)
                .param("publishedAt", Timestamp.from(publishedAt))
                .param("tenantId", tenantId.value())
                .param("idempotencyKey", idempotencyKey)
                .update();
        if (updated != 1) throw new DeadLetterReplayConflictException("Dead-letter replay claim is no longer active");
    }

    @Override
    @Transactional
    public void markFailed(TenantId tenantId, UUID idempotencyKey, String failureClass, Instant failedAt) {
        jdbc.sql("""
                        UPDATE dlq_replay_request
                           SET status = 'FAILED', lease_expires_at = NULL,
                               failure_class = :failureClass, updated_at = :failedAt
                         WHERE tenant_id = :tenantId
                           AND idempotency_key = :idempotencyKey
                           AND status = 'PUBLISHING'
                        """)
                .param("failureClass", failureClass)
                .param("failedAt", Timestamp.from(failedAt))
                .param("tenantId", tenantId.value())
                .param("idempotencyKey", idempotencyKey)
                .update();
    }

    private DeadLetterReplayClaim reclaim(
            ReplayRow row,
            DeadLetterRecord record,
            Instant now,
            Duration leaseDuration
    ) {
        if (!row.matches(record)) {
            throw new DeadLetterReplayConflictException("Idempotency-Key is bound to another dead-letter record");
        }
        if (row.status().equals("PUBLISHED")) {
            return new DeadLetterReplayClaim(
                    row.idempotencyKey(), DeadLetterReplayClaim.State.ALREADY_PUBLISHED, row.publishedAt()
            );
        }
        if (row.status().equals("PUBLISHING") && row.leaseExpiresAt().isAfter(now)) {
            return new DeadLetterReplayClaim(row.idempotencyKey(), DeadLetterReplayClaim.State.IN_PROGRESS, null);
        }

        int updated = jdbc.sql("""
                        UPDATE dlq_replay_request
                           SET status = 'PUBLISHING', attempt_count = attempt_count + 1,
                               lease_expires_at = :leaseExpiresAt, failure_class = NULL,
                               published_at = NULL, updated_at = :now
                         WHERE tenant_id = :tenantId AND idempotency_key = :idempotencyKey
                        """)
                .param("leaseExpiresAt", Timestamp.from(now.plus(leaseDuration)))
                .param("now", Timestamp.from(now))
                .param("tenantId", row.tenantId())
                .param("idempotencyKey", row.idempotencyKey())
                .update();
        if (updated != 1) throw new DeadLetterReplayConflictException("Dead-letter replay claim changed concurrently");
        return new DeadLetterReplayClaim(row.idempotencyKey(), DeadLetterReplayClaim.State.ACQUIRED, null);
    }

    private Optional<ReplayRow> findByKeyForUpdate(TenantId tenantId, UUID idempotencyKey) {
        return jdbc.sql("""
                        SELECT tenant_id, idempotency_key, dlq_topic, dlq_partition, dlq_offset,
                               dlq_record_id, source_topic, status, lease_expires_at, published_at
                          FROM dlq_replay_request
                         WHERE tenant_id = :tenantId AND idempotency_key = :idempotencyKey
                         FOR UPDATE
                        """)
                .param("tenantId", tenantId.value())
                .param("idempotencyKey", idempotencyKey)
                .query(this::map)
                .optional();
    }

    private Optional<ReplayRow> findByLocationForUpdate(TenantId tenantId, DeadLetterRecord record) {
        return jdbc.sql("""
                        SELECT tenant_id, idempotency_key, dlq_topic, dlq_partition, dlq_offset,
                               dlq_record_id, source_topic, status, lease_expires_at, published_at
                          FROM dlq_replay_request
                         WHERE tenant_id = :tenantId AND dlq_topic = :topic
                           AND dlq_partition = :partition AND dlq_offset = :offset
                         FOR UPDATE
                        """)
                .param("tenantId", tenantId.value())
                .param("topic", record.location().topic())
                .param("partition", record.location().partition())
                .param("offset", record.location().offset())
                .query(this::map)
                .optional();
    }

    private ReplayRow map(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        Timestamp lease = rs.getTimestamp("lease_expires_at");
        Timestamp published = rs.getTimestamp("published_at");
        return new ReplayRow(
                rs.getString("tenant_id"), rs.getObject("idempotency_key", UUID.class),
                rs.getString("dlq_topic"), rs.getInt("dlq_partition"), rs.getLong("dlq_offset"),
                rs.getString("dlq_record_id"), rs.getString("source_topic"), rs.getString("status"),
                lease == null ? Instant.EPOCH : lease.toInstant(),
                published == null ? null : published.toInstant()
        );
    }

    private record ReplayRow(
            String tenantId,
            UUID idempotencyKey,
            String topic,
            int partition,
            long offset,
            String recordId,
            String sourceTopic,
            String status,
            Instant leaseExpiresAt,
            Instant publishedAt
    ) {
        boolean matches(DeadLetterRecord record) {
            return topic.equals(record.location().topic())
                    && partition == record.location().partition()
                    && offset == record.location().offset()
                    && recordId.equals(record.recordId())
                    && sourceTopic.equals(record.sourceTopic());
        }
    }
}
