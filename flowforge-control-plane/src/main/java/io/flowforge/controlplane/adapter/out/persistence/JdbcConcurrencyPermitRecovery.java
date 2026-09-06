package io.flowforge.controlplane.adapter.out.persistence;

import io.flowforge.application.coordination.CoordinationPermit;
import io.flowforge.application.coordination.CoordinationPermitLedger;
import io.flowforge.application.coordination.CoordinationPermitService;
import io.flowforge.application.execution.ConcurrencyPermitRecovery;
import io.flowforge.domain.tenancy.TenantId;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Repository
public class JdbcConcurrencyPermitRecovery implements ConcurrencyPermitRecovery {
    private final JdbcClient jdbc;
    private final CoordinationPermitLedger ledger;
    private final ObjectProvider<CoordinationPermitService> permitService;
    private final Duration leaseDuration;

    public JdbcConcurrencyPermitRecovery(
            JdbcClient jdbc,
            CoordinationPermitLedger ledger,
            ObjectProvider<CoordinationPermitService> permitService,
            @Value("${flowforge.concurrency.lease-duration:30s}") Duration leaseDuration
    ) {
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("Concurrency lease duration must be positive");
        }
        this.jdbc = jdbc;
        this.ledger = ledger;
        this.permitService = permitService;
        this.leaseDuration = leaseDuration;
    }

    @Override
    @Transactional
    public int reconcileConcurrencyPermits(int limit, Instant now) {
        if (limit < 1 || limit > 1_000) {
            throw new IllegalArgumentException("Concurrency reconciliation limit must be between 1 and 1000");
        }
        Set<TenantResource> changedResources = new LinkedHashSet<>();
        int changes = releaseOrphans(limit, now, changedResources);

        int workflowLimit = Math.max(0, limit - changes);
        List<WorkflowPermitState> workflows = jdbc.sql("""
                SELECT we.id, we.tenant_id, we.workflow_version_id, we.concurrency_permit_token,
                       v.max_concurrent_executions
                  FROM workflow_execution we
                  JOIN workflow_version v ON v.id = we.workflow_version_id
                 WHERE we.status IN ('PENDING', 'RUNNING', 'CANCELLING')
                   AND v.max_concurrent_executions IS NOT NULL
                 ORDER BY we.workflow_version_id, we.id
                 LIMIT :limit
                 FOR UPDATE OF we SKIP LOCKED
                """)
                .param("limit", workflowLimit)
                .query((rs, rowNum) -> new WorkflowPermitState(
                        rs.getObject("id", UUID.class),
                        new TenantId(rs.getString("tenant_id")),
                        rs.getObject("workflow_version_id", UUID.class),
                        rs.getObject("concurrency_permit_token", UUID.class),
                        rs.getInt("max_concurrent_executions")
                ))
                .list();
        for (WorkflowPermitState state : workflows) {
            String resource = workflowResource(state.workflowVersionId());
            UUID token = restore(
                    state.tenantId(), resource, state.id().toString(), state.token(), state.limit(), now
            );
            if (token != null && !token.equals(state.token())) {
                jdbc.sql("""
                        UPDATE workflow_execution
                           SET concurrency_permit_token = :token
                         WHERE id = :id
                           AND tenant_id = :tenantId
                           AND concurrency_permit_token IS NOT DISTINCT FROM :oldToken
                        """)
                        .param("tenantId", state.tenantId().value())
                        .param("token", token)
                        .param("id", state.id())
                        .param("oldToken", state.token())
                        .update();
            }
            if (token != null) {
                changes++;
                changedResources.add(new TenantResource(state.tenantId(), resource));
            }
        }

        int remaining = Math.max(0, limit - changes);
        if (remaining > 0) {
            List<TaskPermitState> attempts = jdbc.sql("""
                    SELECT ta.id, ta.concurrency_permit_token, we.tenant_id, we.workflow_version_id,
                           te.task_key, wt.max_concurrency
                      FROM task_attempt ta
                      JOIN task_execution te ON te.id = ta.task_execution_id
                      JOIN workflow_execution we ON we.id = te.workflow_execution_id
                      JOIN workflow_task wt
                        ON wt.workflow_version_id = we.workflow_version_id
                       AND wt.task_key = te.task_key
                     WHERE ta.status = 'RUNNING'
                       AND te.status = 'RUNNING'
                       AND wt.max_concurrency IS NOT NULL
                     ORDER BY we.workflow_version_id, te.task_key, ta.id
                     LIMIT :limit
                     FOR UPDATE OF ta SKIP LOCKED
                    """)
                    .param("limit", remaining)
                    .query((rs, rowNum) -> new TaskPermitState(
                            rs.getObject("id", UUID.class),
                            new TenantId(rs.getString("tenant_id")),
                            rs.getObject("workflow_version_id", UUID.class),
                            rs.getString("task_key"),
                            rs.getObject("concurrency_permit_token", UUID.class),
                            rs.getInt("max_concurrency")
                    ))
                    .list();
            for (TaskPermitState state : attempts) {
                String resource = taskResource(state.workflowVersionId(), state.taskKey());
                UUID token = restore(
                        state.tenantId(), resource, state.id().toString(), state.token(), state.limit(), now
                );
                if (token != null && !token.equals(state.token())) {
                    jdbc.sql("""
                        UPDATE task_attempt
                           SET concurrency_permit_token = :token
                          FROM task_execution task
                          JOIN workflow_execution execution
                            ON execution.id = task.workflow_execution_id
                         WHERE task_attempt.id = :id
                           AND task.id = task_attempt.task_execution_id
                           AND execution.tenant_id = :tenantId
                           AND concurrency_permit_token IS NOT DISTINCT FROM :oldToken
                            """)
                            .param("tenantId", state.tenantId().value())
                            .param("token", token)
                            .param("id", state.id())
                            .param("oldToken", state.token())
                            .update();
                }
                if (token != null) {
                    changes++;
                    changedResources.add(new TenantResource(state.tenantId(), resource));
                }
            }
        }
        afterCommit(changedResources);
        return changes;
    }

    private int releaseOrphans(int limit, Instant now, Set<TenantResource> changedResources) {
        List<TenantToken> tokens = jdbc.sql("""
                SELECT cp.tenant_id, cp.token
                  FROM coordination_permit cp
                 WHERE cp.status = 'ACTIVE'
                   AND (
                        (cp.resource_key LIKE 'concurrency:workflow-version:%'
                         AND NOT EXISTS (
                             SELECT 1 FROM workflow_execution we
                              WHERE we.tenant_id = cp.tenant_id
                                AND we.id::text = cp.holder_id
                                AND we.concurrency_permit_token = cp.token
                                AND we.status IN ('PENDING', 'RUNNING', 'CANCELLING')
                         ))
                        OR
                        (cp.resource_key LIKE 'concurrency:task:%'
                         AND NOT EXISTS (
                             SELECT 1 FROM task_attempt ta
                              JOIN task_execution te ON te.id = ta.task_execution_id
                              JOIN workflow_execution we ON we.id = te.workflow_execution_id
                              WHERE we.tenant_id = cp.tenant_id
                                AND ta.id::text = cp.holder_id
                                AND ta.concurrency_permit_token = cp.token
                                AND ta.status = 'RUNNING'
                                AND te.status = 'RUNNING'
                         ))
                   )
                 ORDER BY cp.resource_key, cp.token
                 LIMIT :limit
                """)
                .param("limit", limit)
                .query((rs, rowNumber) -> new TenantToken(
                        new TenantId(rs.getString("tenant_id")),
                        rs.getObject("token", UUID.class)
                ))
                .list();
        int released = 0;
        for (TenantToken token : tokens) {
            Optional<CoordinationPermit> permit = ledger.release(token.tenantId(), token.token(), now);
            if (permit.isPresent()) {
                released++;
                changedResources.add(new TenantResource(
                        permit.get().tenantId(), permit.get().resourceKey()
                ));
            }
        }
        return released;
    }

    private UUID restore(
            TenantId tenantId,
            String resource,
            String holder,
            UUID token,
            int limit,
            Instant now
    ) {
        if (token != null) {
            Optional<CoordinationPermit> renewed = ledger.renew(tenantId, token, now, leaseDuration);
            if (renewed.isPresent()) return renewed.get().token();
        }
        return ledger.tryAcquire(tenantId, resource, holder, UUID.randomUUID(), limit, now, leaseDuration)
                .map(CoordinationPermit::token)
                .orElse(null);
    }

    private void afterCommit(Set<TenantResource> resources) {
        CoordinationPermitService service = permitService.getIfAvailable();
        if (service == null || resources.isEmpty()) return;
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                resources.forEach(resource -> service.reconcile(
                        resource.tenantId(), resource.resourceKey()
                ));
            }
        });
    }

    private static String workflowResource(UUID workflowVersionId) {
        return "concurrency:workflow-version:" + workflowVersionId;
    }

    private static String taskResource(UUID workflowVersionId, String taskKey) {
        return "concurrency:task:" + workflowVersionId + ":" + taskKey;
    }

    private record WorkflowPermitState(
            UUID id,
            TenantId tenantId,
            UUID workflowVersionId,
            UUID token,
            int limit
    ) {
    }

    private record TaskPermitState(
            UUID id,
            TenantId tenantId,
            UUID workflowVersionId,
            String taskKey,
            UUID token,
            int limit
    ) {
    }

    private record TenantResource(TenantId tenantId, String resourceKey) {
    }

    private record TenantToken(TenantId tenantId, UUID token) {
    }
}
