package io.flowforge.controlplane;

import io.flowforge.application.coordination.CoordinationPermit;
import io.flowforge.application.coordination.CoordinationPermitLedger;
import io.flowforge.application.coordination.CoordinationPermitService;
import io.flowforge.application.coordination.EphemeralPermitStore;
import io.flowforge.application.coordination.TokenBucketPolicy;
import io.flowforge.application.coordination.TokenBucketRateLimiter;
import io.flowforge.application.execution.ConcurrencyPermitRecovery;
import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.execution.TaskCompletion;
import io.flowforge.application.execution.TaskResult;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.application.tenancy.TenantQuotaPolicy;
import io.flowforge.application.tenancy.TenantQuotaService;
import io.flowforge.controlplane.adapter.out.coordination.RedisCoordinationKeyspace;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import io.flowforge.domain.tenancy.TenantId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "flowforge.coordination.enabled=true",
        "flowforge.coordination.namespace=integration",
        "flowforge.coordination.lease-duration=5s",
        "flowforge.coordination.ttl-padding=10s",
        "flowforge.scheduling.enabled=false",
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false",
        "flowforge.leases.reaper-enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class CoordinationIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.4-alpine")
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void infrastructureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired
    CoordinationPermitService service;

    @Autowired
    CoordinationPermitLedger ledger;

    @Autowired
    TokenBucketRateLimiter rateLimiter;

    @Autowired
    EphemeralPermitStore store;

    @Autowired
    StringRedisTemplate redis;

    @Autowired
    RedisCoordinationKeyspace keyspace;

    @Autowired
    WorkflowService workflows;

    @Autowired
    ExecutionRepository executions;

    @Autowired
    ConcurrencyPermitRecovery concurrencyRecovery;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    TenantQuotaService quotas;

    @Test
    void keepsConfiguredQuotaRateStateAuthoritativeAfterTotalRedisLoss() {
        TenantId tenant = registerTenant("quota-redis-loss", "Quota Redis Loss");
        TokenBucketPolicy strictRate = new TokenBucketPolicy(2, 1, Duration.ofHours(1));
        quotas.update(tenant, new TenantQuotaPolicy(
                100, 100, 100, 100, strictRate, strictRate
        ), 0);
        String bucket = uniqueResource("quota-rate");
        String redisKey = keyspace.rateLimitKey(tenant, bucket);
        Instant now = Instant.now();

        assertThat(rateLimiter.consume(
                tenant, bucket, quotas.get(tenant).policy().dispatchRateLimit(), 2, now
        ).granted()).isEqualTo(2);
        assertThat(redis.hasKey(redisKey)).isTrue();

        redis.getConnectionFactory().getConnection().serverCommands().flushDb();
        assertThat(redis.hasKey(redisKey)).isFalse();
        assertThat(rateLimiter.consume(
                tenant, bucket, quotas.get(tenant).policy().dispatchRateLimit(), 1, now
        ).granted()).isZero();
        assertThat(redis.opsForHash().get(redisKey, "availableTokens")).isEqualTo("0.0");
    }

    @Test
    void isolatesPermitsRateBucketsAndRedisKeysByTenant() {
        TenantId tenantA = registerTenant("coordination-merchant-a", "Coordination Merchant A");
        TenantId tenantB = registerTenant("coordination-merchant-b", "Coordination Merchant B");
        String resource = uniqueResource("shared-resource");
        CoordinationPermit permitA = service.tryAcquire(tenantA, resource, "same-holder", 1)
                .orElseThrow();
        CoordinationPermit permitB = service.tryAcquire(tenantB, resource, "same-holder", 1)
                .orElseThrow();

        assertThat(permitA.token()).isNotEqualTo(permitB.token());
        assertThat(service.tryAcquire(tenantA, resource, "blocked-holder", 1)).isEmpty();
        assertThat(service.tryAcquire(tenantB, resource, "blocked-holder", 1)).isEmpty();
        assertThat(service.renew(tenantB, permitA.token())).isEmpty();
        assertThat(store.activeCount(tenantA, resource, Instant.now())).isEqualTo(1);
        assertThat(store.activeCount(tenantB, resource, Instant.now())).isEqualTo(1);
        assertThat(keyspace.permitKey(tenantA, resource))
                .isNotEqualTo(keyspace.permitKey(tenantB, resource));

        String bucket = uniqueResource("shared-rate-bucket");
        TokenBucketPolicy policy = new TokenBucketPolicy(2, 1, Duration.ofHours(1));
        Instant now = Instant.now();
        assertThat(rateLimiter.consume(tenantA, bucket, policy, 2, now).granted()).isEqualTo(2);
        assertThat(rateLimiter.consume(tenantB, bucket, policy, 2, now).granted()).isEqualTo(2);
        assertThat(rateLimiter.consume(tenantA, bucket, policy, 1, now).granted()).isZero();
        assertThat(rateLimiter.consume(tenantB, bucket, policy, 1, now).granted()).isZero();
        assertThat(keyspace.rateLimitKey(tenantA, bucket))
                .isNotEqualTo(keyspace.rateLimitKey(tenantB, bucket));
        assertThat(redis.hasKey(keyspace.rateLimitKey(tenantA, bucket))).isTrue();
        assertThat(redis.hasKey(keyspace.rateLimitKey(tenantB, bucket))).isTrue();
    }

    @Test
    void rebuildsVersionedRedisRateLimitStateFromThePostgresBucket() {
        String bucket = "integration/rate-limit/" + UUID.randomUUID();
        String redisKey = keyspace.rateLimitKey(TenantId.LOCAL, bucket);
        Instant now = Instant.now();
        TokenBucketPolicy policy = new TokenBucketPolicy(3, 1, Duration.ofHours(1));

        assertThat(rateLimiter.consume(TenantId.LOCAL, bucket, policy, 2, now).granted()).isEqualTo(2);
        assertThat(redis.opsForHash().get(redisKey, "availableTokens")).isEqualTo("1.0");
        assertThat(redis.getExpire(redisKey)).isPositive();

        redis.delete(redisKey);
        assertThat(redis.hasKey(redisKey)).isFalse();
        assertThat(rateLimiter.consume(TenantId.LOCAL, bucket, policy, 1, now).granted()).isEqualTo(1);

        assertThat(redis.opsForHash().get(redisKey, "availableTokens")).isEqualTo("0.0");
        assertThat(redis.opsForHash().get(redisKey, "stateVersion")).isEqualTo("2");
    }

    @Test
    void mirrorsExecutionPermitsAndRebuildsThemFromActiveDatabaseState() {
        WorkflowDefinition draft = workflows.create(TenantId.LOCAL, new WorkflowDraft(
                "Redis mirrored execution",
                null,
                List.of(new TaskDefinition("ROOT", "Root", "NOOP", Map.of())),
                List.of(),
                1
        ));
        WorkflowDefinition published = workflows.publish(TenantId.LOCAL, draft.id(), draft.lockVersion());
        var execution = executions.start(
                TenantId.LOCAL, published.id(), "redis-execution-" + UUID.randomUUID(), Instant.now()
        );
        UUID versionId = jdbc.sql("SELECT workflow_version_id FROM workflow_execution WHERE id = :id")
                .param("id", execution.workflow().id())
                .query(UUID.class)
                .single();
        String resource = "concurrency:workflow-version:" + versionId;
        assertThat(store.activeCount(TenantId.LOCAL, resource, Instant.now())).isEqualTo(1);

        redis.getConnectionFactory().getConnection().serverCommands().flushDb();
        assertThat(store.activeCount(TenantId.LOCAL, resource, Instant.now())).isZero();
        concurrencyRecovery.reconcileConcurrencyPermits(100, Instant.now());
        assertThat(store.activeCount(TenantId.LOCAL, resource, Instant.now())).isEqualTo(1);

        var task = executions.claimReadyTasks(TenantId.LOCAL, 1, Instant.now()).getFirst();
        executions.completeTask(TaskCompletion.from(task, TaskResult.succeeded()), Instant.now());
        assertThat(store.activeCount(TenantId.LOCAL, resource, Instant.now())).isZero();
    }

    @Test
    void durableLedgerEnforcesCapacityAndIdempotentHolderOwnership() {
        String resource = uniqueResource("capacity");
        CoordinationPermit first = service.tryAcquire(TenantId.LOCAL, resource, "holder-a", 2).orElseThrow();
        CoordinationPermit duplicate = service.tryAcquire(TenantId.LOCAL, resource, "holder-a", 2).orElseThrow();
        CoordinationPermit second = service.tryAcquire(TenantId.LOCAL, resource, "holder-b", 2).orElseThrow();

        assertThat(duplicate.token()).isEqualTo(first.token());
        assertThat(service.tryAcquire(TenantId.LOCAL, resource, "holder-c", 2)).isEmpty();
        assertThat(store.activeCount(TenantId.LOCAL, resource, Instant.now())).isEqualTo(2);
        assertThat(service.release(new CoordinationPermit(
                TenantId.LOCAL, resource, "holder-a", UUID.randomUUID(), first.expiresAt()
        ))).isFalse();
        assertThat(service.release(first)).isTrue();
        assertThat(service.tryAcquire(TenantId.LOCAL, resource, "holder-c", 2)).isPresent();
        assertThat(ledger.findActive(TenantId.LOCAL, resource, Instant.now()))
                .extracting(CoordinationPermit::token)
                .containsExactlyInAnyOrder(second.token(),
                        service.tryAcquire(TenantId.LOCAL, resource, "holder-c", 2).orElseThrow().token());
    }

    @Test
    void rebuildsTheEphemeralViewAfterTotalRedisKeyLoss() {
        String resource = uniqueResource("reconcile");
        service.tryAcquire(TenantId.LOCAL, resource, "holder-a", 3).orElseThrow();
        service.tryAcquire(TenantId.LOCAL, resource, "holder-b", 3).orElseThrow();
        assertThat(store.activeCount(TenantId.LOCAL, resource, Instant.now())).isEqualTo(2);

        redis.getConnectionFactory().getConnection().serverCommands().flushDb();

        assertThat(store.activeCount(TenantId.LOCAL, resource, Instant.now())).isZero();
        assertThat(service.reconcile(TenantId.LOCAL, resource)).isEqualTo(2);
        assertThat(store.activeCount(TenantId.LOCAL, resource, Instant.now())).isEqualTo(2);
        Long ttl = redis.getExpire(keyspace.permitKey(TenantId.LOCAL, resource), TimeUnit.MILLISECONDS);
        assertThat(ttl).isNotNull().isPositive().isLessThanOrEqualTo(15_000L);
    }

    @Test
    void redisLuaAcquireIsAtomicAcrossCompetingCallers() throws Exception {
        String resource = uniqueResource("redis-atomic");
        Instant now = Instant.now();
        int callers = 12;
        CountDownLatch start = new CountDownLatch(1);
        List<Boolean> results;
        try (var pool = Executors.newFixedThreadPool(callers)) {
            var futures = java.util.stream.IntStream.range(0, callers)
                    .mapToObj(index -> pool.submit(() -> {
                        start.await();
                        return store.tryAcquire(
                                new CoordinationPermit(
                                        TenantId.LOCAL,
                                        resource,
                                        "holder-" + index,
                                        UUID.randomUUID(),
                                        now.plusSeconds(5)
                                ),
                                3,
                                now,
                                Duration.ofSeconds(10)
                        );
                    }))
                    .toList();
            start.countDown();
            results = futures.stream().map(future -> {
                try {
                    return future.get(10, TimeUnit.SECONDS);
                } catch (Exception failure) {
                    throw new AssertionError(failure);
                }
            }).toList();
        }

        assertThat(results).filteredOn(Boolean::booleanValue).hasSize(3);
        assertThat(store.activeCount(TenantId.LOCAL, resource, now)).isEqualTo(3);
    }

    @Test
    void durableCapacityIsSerializedAcrossCompetingCallers() throws Exception {
        String resource = uniqueResource("postgres-atomic");
        Instant now = Instant.now();
        int callers = 12;
        CountDownLatch start = new CountDownLatch(1);
        List<Optional<CoordinationPermit>> results;
        try (var pool = Executors.newFixedThreadPool(callers)) {
            var futures = java.util.stream.IntStream.range(0, callers)
                    .mapToObj(index -> pool.submit(() -> {
                        start.await();
                        return ledger.tryAcquire(
                                TenantId.LOCAL,
                                resource,
                                "holder-" + index,
                                UUID.randomUUID(),
                                3,
                                now,
                                Duration.ofSeconds(5)
                        );
                    }))
                    .toList();
            start.countDown();
            results = futures.stream().map(future -> {
                try {
                    return future.get(10, TimeUnit.SECONDS);
                } catch (Exception failure) {
                    throw new AssertionError(failure);
                }
            }).toList();
        }

        assertThat(results).filteredOn(Optional::isPresent).hasSize(3);
        assertThat(ledger.findActive(TenantId.LOCAL, resource, now)).hasSize(3);
    }

    @Test
    void expiredDurablePermitsAreReclaimedWithoutRedisState() {
        String resource = uniqueResource("expiry");
        Instant time = Instant.parse("2026-09-05T12:00:00Z");
        CoordinationPermit first = ledger.tryAcquire(
                TenantId.LOCAL,
                resource, "holder-a", UUID.randomUUID(), 1, time, Duration.ofSeconds(5)
        ).orElseThrow();
        Optional<CoordinationPermit> blocked = ledger.tryAcquire(
                TenantId.LOCAL,
                resource, "holder-b", UUID.randomUUID(), 1, time.plusSeconds(4), Duration.ofSeconds(5)
        );
        Optional<CoordinationPermit> replacement = ledger.tryAcquire(
                TenantId.LOCAL,
                resource, "holder-b", UUID.randomUUID(), 1, time.plusSeconds(5), Duration.ofSeconds(5)
        );

        assertThat(blocked).isEmpty();
        assertThat(replacement).isPresent();
        assertThat(ledger.renew(
                TenantId.LOCAL, first.token(), time.plusSeconds(5), Duration.ofSeconds(5)
        )).isEmpty();
    }

    private static String uniqueResource(String prefix) {
        return prefix + "/" + UUID.randomUUID();
    }

    private TenantId registerTenant(String tenantId, String displayName) {
        jdbc.sql("""
                INSERT INTO tenant_registry(tenant_id, display_name, status)
                VALUES (:tenantId, :displayName, 'ACTIVE')
                ON CONFLICT (tenant_id) DO NOTHING
                """)
                .param("tenantId", tenantId)
                .param("displayName", displayName)
                .update();
        return new TenantId(tenantId);
    }
}
