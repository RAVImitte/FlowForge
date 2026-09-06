package io.flowforge.worker.persistence;

import io.flowforge.observability.TraceContextSnapshot;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public class WorkerResultOutboxRepository {
    private final JdbcClient jdbc;

    public WorkerResultOutboxRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public List<WorkerResultMessage> claimBatch(
            int limit,
            String workerId,
            Instant now,
            Duration leaseDuration
    ) {
        UUID claimToken = UUID.randomUUID();
        return jdbc.sql("""
                WITH candidates AS (
                    SELECT id
                      FROM worker_result_outbox
                     WHERE (status = 'PENDING' AND available_at <= :now)
                        OR (status = 'IN_FLIGHT' AND claimed_until <= :now)
                     ORDER BY created_at, id
                     FOR UPDATE SKIP LOCKED
                     LIMIT :limit
                )
                UPDATE worker_result_outbox outbox
                   SET status = 'IN_FLIGHT',
                       attempt_count = outbox.attempt_count + 1,
                       claimed_by = :workerId,
                       claimed_at = :now,
                       claimed_until = :claimedUntil,
                       claim_token = :claimToken,
                       last_error = NULL
                  FROM candidates
                 WHERE outbox.id = candidates.id
                RETURNING outbox.tenant_id, outbox.id, outbox.workflow_execution_id, outbox.topic,
                          outbox.record_key, outbox.event_type, outbox.schema_version,
                          outbox.payload::text, outbox.attempt_count, outbox.claim_token,
                          outbox.trace_parent, outbox.trace_state, outbox.trace_baggage
                """)
                .param("now", Timestamp.from(now))
                .param("claimedUntil", Timestamp.from(now.plus(leaseDuration)))
                .param("limit", limit)
                .param("workerId", workerId)
                .param("claimToken", claimToken)
                .query((rs, rowNum) -> new WorkerResultMessage(
                        rs.getString("tenant_id"),
                        rs.getObject("id", UUID.class),
                        rs.getObject("workflow_execution_id", UUID.class),
                        rs.getString("topic"),
                        rs.getString("record_key"),
                        rs.getString("event_type"),
                        rs.getInt("schema_version"),
                        rs.getString("payload"),
                        rs.getInt("attempt_count"),
                        rs.getObject("claim_token", UUID.class),
                        new TraceContextSnapshot(
                                rs.getString("trace_parent"),
                                rs.getString("trace_state"),
                                rs.getString("trace_baggage")
                        )
                ))
                .list();
    }

    @Transactional
    public boolean markPublished(UUID id, UUID claimToken, Instant now) {
        return jdbc.sql("""
                UPDATE worker_result_outbox
                   SET status = 'PUBLISHED', published_at = :publishedAt,
                       claimed_by = NULL, claimed_at = NULL, claimed_until = NULL,
                       claim_token = NULL, last_error = NULL
                 WHERE id = :id AND status = 'IN_FLIGHT' AND claim_token = :claimToken
                """)
                .param("publishedAt", Timestamp.from(now))
                .param("id", id)
                .param("claimToken", claimToken)
                .update() == 1;
    }

    @Transactional
    public boolean release(UUID id, UUID claimToken, Instant now, String error) {
        return jdbc.sql("""
                UPDATE worker_result_outbox
                   SET status = 'PENDING', available_at = :availableAt,
                       claimed_by = NULL, claimed_at = NULL, claimed_until = NULL,
                       claim_token = NULL, last_error = :lastError
                 WHERE id = :id AND status = 'IN_FLIGHT' AND claim_token = :claimToken
                """)
                .param("availableAt", Timestamp.from(now))
                .param("lastError", truncate(error, 2_000))
                .param("id", id)
                .param("claimToken", claimToken)
                .update() == 1;
    }

    public long pendingCount() {
        return jdbc.sql("SELECT COUNT(*) FROM worker_result_outbox WHERE status <> 'PUBLISHED'")
                .query(Long.class)
                .single();
    }

    private static String truncate(String value, int maximumLength) {
        if (value == null || value.length() <= maximumLength) return value;
        return value.substring(0, maximumLength);
    }
}
