package io.flowforge.controlplane;

import io.flowforge.application.coordination.CoordinationPermit;
import io.flowforge.application.coordination.CoordinationPermitLedger;
import io.flowforge.application.coordination.CoordinationPermitService;
import io.flowforge.application.coordination.EphemeralPermitStore;
import io.flowforge.application.execution.ConcurrencyPermitRecovery;
import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.execution.TaskCompletion;
import io.flowforge.application.execution.TaskResult;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.controlplane.adapter.out.coordination.RedisCoordinationKeyspace;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
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

    @Test
    void mirrorsExecutionPermitsAndRebuildsThemFromActiveDatabaseState() {
        WorkflowDefinition draft = workflows.create(new WorkflowDraft(
                "Redis mirrored execution",
                null,
                List.of(new TaskDefinition("ROOT", "Root", "NOOP", Map.of())),
                List.of(),
                1
        ));
        WorkflowDefinition published = workflows.publish(draft.id(), draft.lockVersion());
        var execution = executions.start(published.id(), "redis-execution-" + UUID.randomUUID(), Instant.now());
        UUID versionId = jdbc.sql("SELECT workflow_version_id FROM workflow_execution WHERE id = :id")
                .param("id", execution.workflow().id())
                .query(UUID.class)
                .single();
        String resource = "concurrency:workflow-version:" + versionId;
        assertThat(store.activeCount(resource, Instant.now())).isEqualTo(1);

        redis.getConnectionFactory().getConnection().serverCommands().flushDb();
        assertThat(store.activeCount(resource, Instant.now())).isZero();
        concurrencyRecovery.reconcileConcurrencyPermits(100, Instant.now());
        assertThat(store.activeCount(resource, Instant.now())).isEqualTo(1);

        var task = executions.claimReadyTasks(1, Instant.now()).getFirst();
        executions.completeTask(TaskCompletion.from(task, TaskResult.succeeded()), Instant.now());
        assertThat(store.activeCount(resource, Instant.now())).isZero();
    }

    @Test
    void durableLedgerEnforcesCapacityAndIdempotentHolderOwnership() {
        String resource = uniqueResource("capacity");
        CoordinationPermit first = service.tryAcquire(resource, "holder-a", 2).orElseThrow();
        CoordinationPermit duplicate = service.tryAcquire(resource, "holder-a", 2).orElseThrow();
        CoordinationPermit second = service.tryAcquire(resource, "holder-b", 2).orElseThrow();

        assertThat(duplicate.token()).isEqualTo(first.token());
        assertThat(service.tryAcquire(resource, "holder-c", 2)).isEmpty();
        assertThat(store.activeCount(resource, Instant.now())).isEqualTo(2);
        assertThat(service.release(new CoordinationPermit(
                resource, "holder-a", UUID.randomUUID(), first.expiresAt()
        ))).isFalse();
        assertThat(service.release(first)).isTrue();
        assertThat(service.tryAcquire(resource, "holder-c", 2)).isPresent();
        assertThat(ledger.findActive(resource, Instant.now()))
                .extracting(CoordinationPermit::token)
                .containsExactlyInAnyOrder(second.token(),
                        service.tryAcquire(resource, "holder-c", 2).orElseThrow().token());
    }

    @Test
    void rebuildsTheEphemeralViewAfterTotalRedisKeyLoss() {
        String resource = uniqueResource("reconcile");
        service.tryAcquire(resource, "holder-a", 3).orElseThrow();
        service.tryAcquire(resource, "holder-b", 3).orElseThrow();
        assertThat(store.activeCount(resource, Instant.now())).isEqualTo(2);

        redis.getConnectionFactory().getConnection().serverCommands().flushDb();

        assertThat(store.activeCount(resource, Instant.now())).isZero();
        assertThat(service.reconcile(resource)).isEqualTo(2);
        assertThat(store.activeCount(resource, Instant.now())).isEqualTo(2);
        Long ttl = redis.getExpire(keyspace.permitKey(resource), TimeUnit.MILLISECONDS);
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
        assertThat(store.activeCount(resource, now)).isEqualTo(3);
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
        assertThat(ledger.findActive(resource, now)).hasSize(3);
    }

    @Test
    void expiredDurablePermitsAreReclaimedWithoutRedisState() {
        String resource = uniqueResource("expiry");
        Instant time = Instant.parse("2026-09-05T12:00:00Z");
        CoordinationPermit first = ledger.tryAcquire(
                resource, "holder-a", UUID.randomUUID(), 1, time, Duration.ofSeconds(5)
        ).orElseThrow();
        Optional<CoordinationPermit> blocked = ledger.tryAcquire(
                resource, "holder-b", UUID.randomUUID(), 1, time.plusSeconds(4), Duration.ofSeconds(5)
        );
        Optional<CoordinationPermit> replacement = ledger.tryAcquire(
                resource, "holder-b", UUID.randomUUID(), 1, time.plusSeconds(5), Duration.ofSeconds(5)
        );

        assertThat(blocked).isEmpty();
        assertThat(replacement).isPresent();
        assertThat(ledger.renew(first.token(), time.plusSeconds(5), Duration.ofSeconds(5))).isEmpty();
    }

    private static String uniqueResource(String prefix) {
        return prefix + "/" + UUID.randomUUID();
    }
}
