package io.flowforge.controlplane;

import io.flowforge.application.coordination.TokenBucketDecision;
import io.flowforge.application.coordination.TokenBucketPolicy;
import io.flowforge.application.coordination.TokenBucketRateLimiter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "flowforge.scheduling.enabled=false",
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false",
        "flowforge.leases.reaper-enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
class RateLimitingIntegrationTest {
    private static final Instant NOW = Instant.parse("2026-09-05T12:00:00Z");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    TokenBucketRateLimiter limiter;

    @Autowired
    JdbcClient jdbc;

    @BeforeEach
    void clearBuckets() {
        jdbc.sql("DELETE FROM admission_rate_bucket").update();
    }

    @Test
    void refillsFractionalTokensAndReturnsPreciseRetryGuidance() {
        TokenBucketPolicy policy = new TokenBucketPolicy(2, 2, Duration.ofSeconds(1));

        assertThat(limiter.consume("test/refill", policy, 2, NOW).granted()).isEqualTo(2);
        TokenBucketDecision empty = limiter.consume("test/refill", policy, 1, NOW);
        TokenBucketDecision halfFull = limiter.consume(
                "test/refill", policy, 1, NOW.plusMillis(250)
        );
        TokenBucketDecision refilled = limiter.consume(
                "test/refill", policy, 1, NOW.plusMillis(500)
        );

        assertThat(empty.granted()).isZero();
        assertThat(empty.retryAfter()).isEqualTo(Duration.ofMillis(500));
        assertThat(halfFull.granted()).isZero();
        assertThat(halfFull.retryAfter()).isEqualTo(Duration.ofMillis(250));
        assertThat(refilled.granted()).isEqualTo(1);
    }

    @Test
    void serializesConcurrentConsumersWithoutOversubscribingCapacity() throws Exception {
        TokenBucketPolicy policy = new TokenBucketPolicy(5, 1, Duration.ofHours(1));
        int consumers = 12;
        CountDownLatch ready = new CountDownLatch(consumers);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(consumers)) {
            List<java.util.concurrent.Future<Integer>> results = java.util.stream.IntStream
                    .range(0, consumers)
                    .mapToObj(ignored -> executor.submit(() -> {
                        ready.countDown();
                        start.await(10, TimeUnit.SECONDS);
                        return limiter.consume("test/concurrent", policy, 1, NOW).granted();
                    }))
                    .toList();
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            int granted = 0;
            for (var result : results) granted += result.get(10, TimeUnit.SECONDS);
            assertThat(granted).isEqualTo(5);
        }
    }
}
