package io.flowforge.application.workflow;

import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;

import java.util.Optional;
import java.util.UUID;

public interface WorkflowRepository {
    WorkflowDefinition create(TenantId tenantId, WorkflowDraft draft);

    Optional<WorkflowDefinition> findById(TenantId tenantId, UUID id);

    PageResult<WorkflowDefinition> findAll(TenantId tenantId, int page, int size);

    boolean hasPublishedVersion(TenantId tenantId, UUID id);

    WorkflowDefinition update(TenantId tenantId, UUID id, long expectedLockVersion, WorkflowDraft draft);

    WorkflowDefinition publish(TenantId tenantId, UUID id, long expectedLockVersion);

    void archive(TenantId tenantId, UUID id, long expectedLockVersion);

}
