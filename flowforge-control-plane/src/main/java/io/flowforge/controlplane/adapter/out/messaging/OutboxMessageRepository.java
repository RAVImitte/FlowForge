package io.flowforge.controlplane.adapter.out.messaging;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public class OutboxMessageRepository {
    private final JdbcClient jdbc;

    public OutboxMessageRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public List<OutboxMessage> claimBatch(
            int limit,
            String instanceId,
            Instant now,
            Duration leaseDuration
    ) {
        UUID claimToken = UUID.randomUUID();
        return jdbc.sql("""
                WITH candidates AS (
                    SELECT id
                      FROM control_plane_outbox
                     WHERE (status = 'PENDING' AND available_at <= :now)
                        OR (status = 'IN_FLIGHT' AND claimed_until <= :now)
                     ORDER BY created_at, id
                     FOR UPDATE SKIP LOCKED
                     LIMIT :limit
                )
                UPDATE control_plane_outbox outbox
                   SET status = 'IN_FLIGHT',
                       attempt_count = outbox.attempt_count + 1,
                       claimed_by = :instanceId,
                       claimed_at = :now,
                       claimed_until = :claimedUntil,
                       claim_token = :claimToken,
                       last_error = NULL
                  FROM candidates
                 WHERE outbox.id = candidates.id
                RETURNING outbox.id, outbox.workflow_execution_id, outbox.task_execution_id,
                          outbox.message_kind, outbox.topic, outbox.record_key,
                          outbox.event_type, outbox.schema_version, outbox.payload::text,
                          outbox.attempt_count, outbox.created_at, outbox.claim_token
                """)
                .param("now", timestamp(now))
                .param("claimedUntil", timestamp(now.plus(leaseDuration)))
                .param("limit", limit)
                .param("instanceId", instanceId)
                .param("claimToken", claimToken)
                .query((rs, rowNum) -> new OutboxMessage(
                        rs.getObject("id", UUID.class),
                        rs.getObject("workflow_execution_id", UUID.class),
                        rs.getObject("task_execution_id", UUID.class),
                        rs.getString("message_kind"),
                        rs.getString("topic"),
                        rs.getString("record_key"),
                        rs.getString("event_type"),
                        rs.getInt("schema_version"),
                        rs.getString("payload"),
                        rs.getInt("attempt_count"),
                        instant(rs.getObject("created_at")),
                        rs.getObject("claim_token", UUID.class)
                ))
                .list();
    }

    @Transactional
    public boolean markPublished(UUID id, UUID claimToken, Instant publishedAt) {
        return jdbc.sql("""
                UPDATE control_plane_outbox
                   SET status = 'PUBLISHED',
                       published_at = :publishedAt,
                       claimed_by = NULL,
                       claimed_at = NULL,
                       claimed_until = NULL,
                       claim_token = NULL,
                       last_error = NULL
                 WHERE id = :id
                   AND status = 'IN_FLIGHT'
                   AND claim_token = :claimToken
                """)
                .param("id", id)
                .param("claimToken", claimToken)
                .param("publishedAt", timestamp(publishedAt))
                .update() == 1;
    }

    @Transactional
    public boolean release(UUID id, UUID claimToken, Instant availableAt, String error) {
        return jdbc.sql("""
                UPDATE control_plane_outbox
                   SET status = 'PENDING',
                       available_at = :availableAt,
                       claimed_by = NULL,
                       claimed_at = NULL,
                       claimed_until = NULL,
                       claim_token = NULL,
                       last_error = :lastError
                 WHERE id = :id
                   AND status = 'IN_FLIGHT'
                   AND claim_token = :claimToken
                """)
                .param("id", id)
                .param("claimToken", claimToken)
                .param("availableAt", timestamp(availableAt))
                .param("lastError", truncate(error, 2_000))
                .update() == 1;
    }

    public long pendingCount() {
        return jdbc.sql("""
                SELECT COUNT(*)
                  FROM control_plane_outbox
                 WHERE status <> 'PUBLISHED'
                """)
                .query(Long.class)
                .single();
    }

    private static String truncate(String value, int maximumLength) {
        if (value == null || value.length() <= maximumLength) return value;
        return value.substring(0, maximumLength);
    }

    private static Instant instant(Object value) {
        if (value instanceof OffsetDateTime timestamp) return timestamp.toInstant();
        if (value instanceof Timestamp timestamp) return timestamp.toInstant();
        if (value instanceof Instant instant) return instant;
        throw new IllegalStateException("Unsupported timestamp value: " + value);
    }

    private static Timestamp timestamp(Instant value) {
        return Timestamp.from(value);
    }
}
