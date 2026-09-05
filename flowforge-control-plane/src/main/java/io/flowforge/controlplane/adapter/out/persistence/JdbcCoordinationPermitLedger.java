package io.flowforge.controlplane.adapter.out.persistence;

import io.flowforge.application.coordination.CoordinationPermit;
import io.flowforge.application.coordination.CoordinationPermitLedger;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcCoordinationPermitLedger implements CoordinationPermitLedger {
    private final JdbcClient jdbc;

    public JdbcCoordinationPermitLedger(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    @Transactional
    public Optional<CoordinationPermit> tryAcquire(
            String resourceKey,
            String holderId,
            UUID proposedToken,
            int limit,
            Instant now,
            Duration leaseDuration
    ) {
        String resource = required(resourceKey, "resourceKey", 300);
        String holder = required(holderId, "holderId", 200);
        jdbc.sql("""
                INSERT INTO coordination_resource(resource_key, created_at, updated_at)
                VALUES (:resourceKey, :now, :now)
                ON CONFLICT (resource_key) DO NOTHING
                """)
                .param("resourceKey", resource)
                .param("now", timestamp(now))
                .update();
        lockResource(resource);
        expire(resource, now);

        Optional<CoordinationPermit> existing = findActiveForHolder(resource, holder);
        if (existing.isPresent()) return existing;

        long active = jdbc.sql("""
                SELECT COUNT(*)
                  FROM coordination_permit
                 WHERE resource_key = :resourceKey
                   AND status = 'ACTIVE'
                """)
                .param("resourceKey", resource)
                .query(Long.class)
                .single();
        if (active >= limit) return Optional.empty();

        Instant expiresAt = now.plus(leaseDuration);
        jdbc.sql("""
                INSERT INTO coordination_permit(
                    token, resource_key, holder_id, status,
                    expires_at, created_at, renewed_at, released_at
                ) VALUES (
                    :token, :resourceKey, :holderId, 'ACTIVE',
                    :expiresAt, :now, :now, NULL
                )
                """)
                .param("token", proposedToken)
                .param("resourceKey", resource)
                .param("holderId", holder)
                .param("expiresAt", timestamp(expiresAt))
                .param("now", timestamp(now))
                .update();
        touchResource(resource, now);
        return Optional.of(new CoordinationPermit(resource, holder, proposedToken, expiresAt));
    }

    @Override
    @Transactional
    public Optional<CoordinationPermit> renew(UUID token, Instant now, Duration leaseDuration) {
        Optional<String> resource = findResourceForToken(token);
        if (resource.isEmpty()) return Optional.empty();
        lockResource(resource.get());
        jdbc.sql("""
                UPDATE coordination_permit
                   SET status = 'EXPIRED', released_at = :now
                 WHERE token = :token
                   AND status = 'ACTIVE'
                   AND expires_at <= :now
                """)
                .param("now", timestamp(now))
                .param("token", token)
                .update();
        Optional<CoordinationPermit> renewed = jdbc.sql("""
                UPDATE coordination_permit
                   SET expires_at = :expiresAt,
                       renewed_at = :now
                 WHERE token = :token
                   AND status = 'ACTIVE'
                   AND expires_at > :now
                RETURNING resource_key, holder_id, token, expires_at
                """)
                .param("expiresAt", timestamp(now.plus(leaseDuration)))
                .param("now", timestamp(now))
                .param("token", token)
                .query(this::map)
                .optional();
        if (renewed.isPresent()) touchResource(resource.get(), now);
        return renewed;
    }

    @Override
    @Transactional
    public Optional<CoordinationPermit> release(UUID token, Instant now) {
        Optional<String> resource = findResourceForToken(token);
        if (resource.isEmpty()) return Optional.empty();
        lockResource(resource.get());
        Optional<CoordinationPermit> released = jdbc.sql("""
                UPDATE coordination_permit
                   SET status = 'RELEASED', released_at = :now
                 WHERE token = :token
                   AND status = 'ACTIVE'
                RETURNING resource_key, holder_id, token, expires_at
                """)
                .param("now", timestamp(now))
                .param("token", token)
                .query(this::map)
                .optional();
        if (released.isPresent()) touchResource(resource.get(), now);
        return released;
    }

    @Override
    @Transactional
    public List<CoordinationPermit> findActive(String resourceKey, Instant now) {
        String resource = required(resourceKey, "resourceKey", 300);
        expire(resource, now);
        return jdbc.sql("""
                SELECT resource_key, holder_id, token, expires_at
                  FROM coordination_permit
                 WHERE resource_key = :resourceKey
                   AND status = 'ACTIVE'
                 ORDER BY token
                """)
                .param("resourceKey", resource)
                .query(this::map)
                .list();
    }

    private void lockResource(String resourceKey) {
        jdbc.sql("""
                SELECT resource_key
                  FROM coordination_resource
                 WHERE resource_key = :resourceKey
                 FOR UPDATE
                """)
                .param("resourceKey", resourceKey)
                .query(String.class)
                .single();
    }

    private void expire(String resourceKey, Instant now) {
        jdbc.sql("""
                UPDATE coordination_permit
                   SET status = 'EXPIRED', released_at = :now
                 WHERE resource_key = :resourceKey
                   AND status = 'ACTIVE'
                   AND expires_at <= :now
                """)
                .param("now", timestamp(now))
                .param("resourceKey", resourceKey)
                .update();
    }

    private Optional<CoordinationPermit> findActiveForHolder(String resourceKey, String holderId) {
        return jdbc.sql("""
                SELECT resource_key, holder_id, token, expires_at
                  FROM coordination_permit
                 WHERE resource_key = :resourceKey
                   AND holder_id = :holderId
                   AND status = 'ACTIVE'
                """)
                .param("resourceKey", resourceKey)
                .param("holderId", holderId)
                .query(this::map)
                .optional();
    }

    private Optional<String> findResourceForToken(UUID token) {
        return jdbc.sql("SELECT resource_key FROM coordination_permit WHERE token = :token")
                .param("token", token)
                .query(String.class)
                .optional();
    }

    private void touchResource(String resourceKey, Instant now) {
        jdbc.sql("""
                UPDATE coordination_resource SET updated_at = :now WHERE resource_key = :resourceKey
                """)
                .param("now", timestamp(now))
                .param("resourceKey", resourceKey)
                .update();
    }

    private CoordinationPermit map(ResultSet rs, int rowNumber) throws SQLException {
        return new CoordinationPermit(
                rs.getString("resource_key"),
                rs.getString("holder_id"),
                rs.getObject("token", UUID.class),
                instant(rs.getObject("expires_at"))
        );
    }

    private static String required(String value, String name, int maximumLength) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
        String normalized = value.strip();
        if (normalized.length() > maximumLength) {
            throw new IllegalArgumentException(name + " must not exceed " + maximumLength + " characters");
        }
        return normalized;
    }

    private static Timestamp timestamp(Instant value) {
        return Timestamp.from(value);
    }

    private static Instant instant(Object value) {
        if (value instanceof OffsetDateTime timestamp) return timestamp.toInstant();
        if (value instanceof Timestamp timestamp) return timestamp.toInstant();
        if (value instanceof Instant instant) return instant;
        throw new IllegalStateException("Unsupported timestamp value: " + value);
    }
}
