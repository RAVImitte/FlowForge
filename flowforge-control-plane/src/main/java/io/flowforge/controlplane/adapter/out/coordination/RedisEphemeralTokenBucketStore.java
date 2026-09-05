package io.flowforge.controlplane.adapter.out.coordination;

import io.flowforge.application.coordination.EphemeralTokenBucketStore;
import io.flowforge.application.coordination.TokenBucketSnapshot;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

@Component
@ConditionalOnProperty(prefix = "flowforge.coordination", name = "enabled", havingValue = "true")
public class RedisEphemeralTokenBucketStore implements EphemeralTokenBucketStore {
    private static final DefaultRedisScript<Long> REPLACE_IF_NEWER = new DefaultRedisScript<>("""
            local current = redis.call('HGET', KEYS[1], 'stateVersion')
            if current and tonumber(current) >= tonumber(ARGV[1]) then
              return 0
            end
            redis.call('HSET', KEYS[1],
              'stateVersion', ARGV[1],
              'capacity', ARGV[2],
              'refillTokens', ARGV[3],
              'refillPeriodMs', ARGV[4],
              'availableTokens', ARGV[5],
              'lastRefillAtMs', ARGV[6])
            redis.call('PEXPIRE', KEYS[1], ARGV[7])
            return 1
            """, Long.class);

    private final StringRedisTemplate redis;
    private final RedisCoordinationKeyspace keyspace;

    public RedisEphemeralTokenBucketStore(
            StringRedisTemplate redis,
            RedisCoordinationKeyspace keyspace
    ) {
        this.redis = redis;
        this.keyspace = keyspace;
    }

    @Override
    public void replaceIfNewer(TokenBucketSnapshot snapshot, Duration ttl) {
        Long result = redis.execute(
                REPLACE_IF_NEWER,
                List.of(keyspace.rateLimitKey(snapshot.bucketKey())),
                Long.toString(snapshot.stateVersion()),
                Integer.toString(snapshot.policy().capacity()),
                Integer.toString(snapshot.policy().refillTokens()),
                Long.toString(snapshot.policy().refillPeriod().toMillis()),
                Double.toString(snapshot.availableTokens()),
                Long.toString(snapshot.lastRefillAt().toEpochMilli()),
                Long.toString(Math.max(1, ttl.toMillis()))
        );
        if (result == null) throw new IllegalStateException("Redis rate-limit script returned no result");
    }
}
