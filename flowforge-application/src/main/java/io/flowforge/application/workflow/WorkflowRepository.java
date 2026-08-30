package io.flowforge.application.workflow;

import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;

import java.util.Optional;
import java.util.UUID;

public interface WorkflowRepository {
    WorkflowDefinition create(WorkflowDraft draft);

    Optional<WorkflowDefinition> findById(UUID id);

    PageResult<WorkflowDefinition> findAll(int page, int size);

    WorkflowDefinition update(UUID id, long expectedLockVersion, WorkflowDraft draft);

    WorkflowDefinition publish(UUID id, long expectedLockVersion);

    void archive(UUID id, long expectedLockVersion);
}
