package io.flowforge.controlplane.adapter.out.coordination;

import io.flowforge.application.coordination.CoordinationPermit;
import io.flowforge.application.coordination.EphemeralPermitStore;
import io.flowforge.domain.tenancy.TenantId;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
@ConditionalOnProperty(prefix = "flowforge.coordination", name = "enabled", havingValue = "true")
public class RedisEphemeralPermitStore implements EphemeralPermitStore {
    private static final DefaultRedisScript<Long> ACQUIRE = script("""
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
            if redis.call('ZSCORE', KEYS[1], ARGV[2]) then
              redis.call('ZADD', KEYS[1], ARGV[3], ARGV[2])
              redis.call('PEXPIRE', KEYS[1], ARGV[4])
              return 1
            end
            if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[5]) then
              return 0
            end
            redis.call('ZADD', KEYS[1], ARGV[3], ARGV[2])
            redis.call('PEXPIRE', KEYS[1], ARGV[4])
            return 1
            """);
    private static final DefaultRedisScript<Long> RENEW = script("""
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
            if not redis.call('ZSCORE', KEYS[1], ARGV[2]) then
              return 0
            end
            redis.call('ZADD', KEYS[1], ARGV[3], ARGV[2])
            redis.call('PEXPIRE', KEYS[1], ARGV[4])
            return 1
            """);
    private static final DefaultRedisScript<Long> RELEASE = script("""
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
            local removed = redis.call('ZREM', KEYS[1], ARGV[2])
            if redis.call('ZCARD', KEYS[1]) == 0 then
              redis.call('DEL', KEYS[1])
            else
              redis.call('PEXPIRE', KEYS[1], ARGV[3])
            end
            return removed
            """);
    private static final DefaultRedisScript<Long> REPLACE = script("""
            redis.call('DEL', KEYS[1])
            local index = 2
            while index <= #ARGV do
              redis.call('ZADD', KEYS[1], ARGV[index + 1], ARGV[index])
              index = index + 2
            end
            if #ARGV > 1 then
              redis.call('PEXPIRE', KEYS[1], ARGV[1])
            end
            return (#ARGV - 1) / 2
            """);
    private static final DefaultRedisScript<Long> ACTIVE_COUNT = script("""
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
            return redis.call('ZCARD', KEYS[1])
            """);

    private final StringRedisTemplate redis;
    private final RedisCoordinationKeyspace keyspace;

    public RedisEphemeralPermitStore(StringRedisTemplate redis, RedisCoordinationKeyspace keyspace) {
        this.redis = redis;
        this.keyspace = keyspace;
    }

    @Override
    public boolean tryAcquire(CoordinationPermit permit, int limit, Instant now, Duration ttlPadding) {
        long result = execute(
                ACQUIRE,
                permit.tenantId(),
                permit.resourceKey(),
                Long.toString(now.toEpochMilli()),
                permit.token().toString(),
                Long.toString(permit.expiresAt().toEpochMilli()),
                Long.toString(ttlMillis(List.of(permit), now, ttlPadding)),
                Integer.toString(limit)
        );
        return result == 1;
    }

    @Override
    public boolean renew(CoordinationPermit permit, Instant now, Duration ttlPadding) {
        long result = execute(
                RENEW,
                permit.tenantId(),
                permit.resourceKey(),
                Long.toString(now.toEpochMilli()),
                permit.token().toString(),
                Long.toString(permit.expiresAt().toEpochMilli()),
                Long.toString(ttlMillis(List.of(permit), now, ttlPadding))
        );
        return result == 1;
    }

    @Override
    public boolean release(
            TenantId tenantId,
            String resourceKey,
            UUID token,
            Instant now,
            Duration ttlPadding
    ) {
        long result = execute(
                RELEASE,
                tenantId,
                resourceKey,
                Long.toString(now.toEpochMilli()),
                token.toString(),
                Long.toString(Math.max(1, ttlPadding.toMillis()))
        );
        return result == 1;
    }

    @Override
    public void replace(
            TenantId tenantId,
            String resourceKey,
            List<CoordinationPermit> permits,
            Instant now,
            Duration ttlPadding
    ) {
        List<String> arguments = new ArrayList<>();
        arguments.add(Long.toString(ttlMillis(permits, now, ttlPadding)));
        permits.stream()
                .filter(permit -> permit.expiresAt().isAfter(now))
                .forEach(permit -> {
                    arguments.add(permit.token().toString());
                    arguments.add(Long.toString(permit.expiresAt().toEpochMilli()));
                });
        execute(REPLACE, tenantId, resourceKey, arguments.toArray(String[]::new));
    }

    @Override
    public long activeCount(TenantId tenantId, String resourceKey, Instant now) {
        return execute(ACTIVE_COUNT, tenantId, resourceKey, Long.toString(now.toEpochMilli()));
    }

    private long execute(
            DefaultRedisScript<Long> script,
            TenantId tenantId,
            String resourceKey,
            String... arguments
    ) {
        Long result = redis.execute(
                script, List.of(keyspace.permitKey(tenantId, resourceKey)), (Object[]) arguments
        );
        if (result == null) throw new IllegalStateException("Redis coordination script returned no result");
        return result;
    }

    private static long ttlMillis(
            List<CoordinationPermit> permits,
            Instant now,
            Duration ttlPadding
    ) {
        long latestExpiry = permits.stream()
                .map(CoordinationPermit::expiresAt)
                .mapToLong(Instant::toEpochMilli)
                .max()
                .orElse(now.toEpochMilli());
        long remaining = Math.max(0, latestExpiry - now.toEpochMilli());
        return Math.max(1, Math.addExact(remaining, ttlPadding.toMillis()));
    }

    private static DefaultRedisScript<Long> script(String source) {
        return new DefaultRedisScript<>(source, Long.class);
    }
}
