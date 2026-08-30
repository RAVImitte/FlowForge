package io.flowforge.application.workflow;

import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;

import java.util.Objects;
import java.util.UUID;

public final class WorkflowService {
    private final WorkflowRepository repository;

    public WorkflowService(WorkflowRepository repository) {
        this.repository = Objects.requireNonNull(repository);
    }

    public WorkflowDefinition create(WorkflowDraft draft) {
        return repository.create(draft);
    }

    public WorkflowDefinition get(UUID id) {
        return repository.findById(id).orElseThrow(() -> new WorkflowNotFoundException(id));
    }

    public PageResult<WorkflowDefinition> list(int page, int size) {
        if (page < 0) throw new IllegalArgumentException("page must be at least 0");
        if (size < 1 || size > 100) throw new IllegalArgumentException("size must be between 1 and 100");
        return repository.findAll(page, size);
    }

    public WorkflowDefinition update(UUID id, long expectedLockVersion, WorkflowDraft draft) {
        return repository.update(id, expectedLockVersion, draft);
    }

    public WorkflowDefinition publish(UUID id, long expectedLockVersion) {
        return repository.publish(id, expectedLockVersion);
    }

    public void archive(UUID id, long expectedLockVersion) {
        repository.archive(id, expectedLockVersion);
    }
}
