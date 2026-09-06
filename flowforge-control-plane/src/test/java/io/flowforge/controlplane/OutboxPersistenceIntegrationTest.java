package io.flowforge.controlplane;

import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.application.execution.DurableTaskQueue;
import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.controlplane.adapter.out.messaging.OutboxMessage;
import io.flowforge.controlplane.adapter.out.messaging.OutboxMessageRepository;
import io.flowforge.domain.execution.TaskRunStatus;
import io.flowforge.domain.execution.WorkflowExecution;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.outbox.publisher-enabled=false",
        "flowforge.outbox.command-dispatch-enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
class OutboxPersistenceIntegrationTest {
    private static final Instant TIME = Instant.parse("2026-09-04T12:00:00Z");
    private static final Duration LEASE = Duration.ofSeconds(30);

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    WorkflowService workflowService;

    @Autowired
    ExecutionRepository executionRepository;

    @Autowired
    DurableTaskQueue durableTaskQueue;

    @Autowired
    OutboxMessageRepository outboxRepository;

    @Autowired
    JdbcClient jdbc;

    @BeforeEach
    void removeExecutions() {
        jdbc.sql("DELETE FROM workflow_execution").update();
    }

    @Test
    void writesLifecycleEventsAndTaskCommandWithTheirStateTransitions() {
        WorkflowExecution execution = startWorkflow(List.of(
                new TaskDefinition("ROOT", "Root", "DELAY", Map.of("durationMs", 25))
        ));

        assertEventJournalMatchesOutbox(execution.workflow().id(), execution);

        assertThat(durableTaskQueue.enqueueReadyTasks(
                TenantId.LOCAL, 10, TIME.plusSeconds(1)
        )).isEqualTo(1);

        WorkflowExecution claimed = executionRepository.findById(
                TenantId.LOCAL, execution.workflow().id()
        ).orElseThrow();
        assertThat(claimed.tasks()).singleElement().satisfies(task -> {
            assertThat(task.status()).isEqualTo(TaskRunStatus.RUNNING);
            assertThat(task.stateVersion()).isEqualTo(1);
        });
        assertThat(claimed.attempts()).hasSize(1);
        assertEventJournalMatchesOutbox(claimed.workflow().id(), claimed);

        Map<String, Object> command = jdbc.sql("""
                SELECT record_key,
                       payload ->> 'eventId' AS event_id,
                       payload ->> 'correlationId' AS correlation_id,
                       payload -> 'payload' ->> 'taskKey' AS task_key,
                       payload -> 'payload' ->> 'taskType' AS task_type,
                       payload -> 'payload' ->> 'expectedStateVersion' AS state_version,
                       payload -> 'payload' ->> 'attemptNumber' AS attempt_number,
                       payload -> 'payload' ->> 'fencingToken' AS fencing_token
                  FROM control_plane_outbox
                 WHERE workflow_execution_id = :executionId
                   AND message_kind = 'TASK_COMMAND'
                """)
                .param("executionId", claimed.workflow().id())
                .query()
                .singleRow();

        assertThat(command)
                .containsEntry("record_key", claimed.tasks().getFirst().id().toString())
                .containsEntry("correlation_id", claimed.workflow().id().toString())
                .containsEntry("task_key", "ROOT")
                .containsEntry("task_type", "DELAY")
                .containsEntry("state_version", "1")
                .containsEntry("attempt_number", "1");
        assertThat(command.get("event_id")).isNotNull();
        assertThat(command.get("fencing_token")).isNotNull();
    }

    @Test
    void reclaimsExpiredLeasesWithStableEventIds() {
        startWorkflow(List.of(task("ROOT")));
        durableTaskQueue.enqueueReadyTasks(TenantId.LOCAL, 10, TIME.plusSeconds(1));

        List<OutboxMessage> firstClaim = outboxRepository.claimBatch(
                100, "publisher-a", TIME.plusSeconds(2), LEASE
        );
        List<OutboxMessage> whileLeased = outboxRepository.claimBatch(
                100, "publisher-b", TIME.plusSeconds(31), LEASE
        );
        List<OutboxMessage> recovered = outboxRepository.claimBatch(
                100, "publisher-b", TIME.plusSeconds(32), LEASE
        );

        assertThat(firstClaim).hasSize(5);
        assertThat(whileLeased).isEmpty();
        assertThat(recovered).extracting(OutboxMessage::id)
                .containsExactlyInAnyOrderElementsOf(firstClaim.stream().map(OutboxMessage::id).toList());
        assertThat(recovered).allSatisfy(message -> assertThat(message.attemptCount()).isEqualTo(2));
        assertThat(recovered.getFirst().claimToken()).isNotEqualTo(firstClaim.getFirst().claimToken());
        assertThat(outboxRepository.markPublished(
                recovered.getFirst().id(), firstClaim.getFirst().claimToken(), TIME.plusSeconds(33)
        )).isFalse();
        assertThat(outboxRepository.markPublished(
                recovered.getFirst().id(), recovered.getFirst().claimToken(), TIME.plusSeconds(33)
        )).isTrue();
    }

    @Test
    void concurrentPublishersClaimDisjointBatches() throws Exception {
        startWorkflow(IntStream.range(0, 20).mapToObj(index -> task("ROOT_" + index)).toList());
        assertThat(durableTaskQueue.enqueueReadyTasks(
                TenantId.LOCAL, 100, TIME.plusSeconds(1)
        )).isEqualTo(20);

        CountDownLatch start = new CountDownLatch(1);
        List<OutboxMessage> first;
        List<OutboxMessage> second;
        try (var executor = Executors.newFixedThreadPool(2)) {
            var firstClaim = executor.submit(() -> {
                start.await();
                return outboxRepository.claimBatch(31, "publisher-a", TIME.plusSeconds(2), LEASE);
            });
            var secondClaim = executor.submit(() -> {
                start.await();
                return outboxRepository.claimBatch(31, "publisher-b", TIME.plusSeconds(2), LEASE);
            });
            start.countDown();
            first = firstClaim.get(10, TimeUnit.SECONDS);
            second = secondClaim.get(10, TimeUnit.SECONDS);
        }

        Set<Object> firstIds = new HashSet<>(first.stream().map(OutboxMessage::id).toList());
        Set<Object> secondIds = new HashSet<>(second.stream().map(OutboxMessage::id).toList());
        assertThat(first).hasSize(31);
        assertThat(second).hasSize(31);
        assertThat(firstIds).doesNotContainAnyElementsOf(secondIds);
        assertThat(outboxRepository.pendingCount()).isEqualTo(62);
    }

    private void assertEventJournalMatchesOutbox(java.util.UUID executionId, WorkflowExecution execution) {
        List<java.util.UUID> outboxEventIds = jdbc.sql("""
                SELECT id
                  FROM control_plane_outbox
                 WHERE workflow_execution_id = :executionId
                   AND message_kind = 'EXECUTION_EVENT'
                 ORDER BY created_at, id
                """)
                .param("executionId", executionId)
                .query(java.util.UUID.class)
                .list();
        assertThat(outboxEventIds).containsExactlyInAnyOrderElementsOf(
                execution.events().stream().map(event -> event.id()).toList()
        );
    }

    private WorkflowExecution startWorkflow(List<TaskDefinition> tasks) {
        WorkflowDefinition created = workflowService.create(TenantId.LOCAL, new WorkflowDraft(
                "Outbox test", null, tasks, List.of()
        ));
        WorkflowDefinition published = workflowService.publish(
                TenantId.LOCAL, created.id(), created.lockVersion()
        );
        return executionRepository.start(
                TenantId.LOCAL, published.id(), "request-" + java.util.UUID.randomUUID(), TIME
        );
    }

    private static TaskDefinition task(String key) {
        return new TaskDefinition(key, key, "NOOP", Map.of());
    }
}
