package io.flowforge.application.workflow;

import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;

import java.util.Objects;
import java.util.UUID;

public final class WorkflowService {
    private final WorkflowRepository repository;

    public WorkflowService(WorkflowRepository repository) {
        this.repository = Objects.requireNonNull(repository);
    }

    public WorkflowDefinition create(TenantId tenantId, WorkflowDraft draft) {
        return repository.create(Objects.requireNonNull(tenantId), draft);
    }

    public WorkflowDefinition get(TenantId tenantId, UUID id) {
        return repository.findById(Objects.requireNonNull(tenantId), id)
                .orElseThrow(() -> new WorkflowNotFoundException(id));
    }

    public PageResult<WorkflowDefinition> list(TenantId tenantId, int page, int size) {
        if (page < 0) throw new IllegalArgumentException("page must be at least 0");
        if (size < 1 || size > 100) throw new IllegalArgumentException("size must be between 1 and 100");
        return repository.findAll(Objects.requireNonNull(tenantId), page, size);
    }

    public WorkflowDefinition update(
            TenantId tenantId,
            UUID id,
            long expectedLockVersion,
            WorkflowDraft draft
    ) {
        return repository.update(Objects.requireNonNull(tenantId), id, expectedLockVersion, draft);
    }

    public WorkflowDefinition publish(TenantId tenantId, UUID id, long expectedLockVersion) {
        return repository.publish(Objects.requireNonNull(tenantId), id, expectedLockVersion);
    }

    public void archive(TenantId tenantId, UUID id, long expectedLockVersion) {
        repository.archive(Objects.requireNonNull(tenantId), id, expectedLockVersion);
    }
}
