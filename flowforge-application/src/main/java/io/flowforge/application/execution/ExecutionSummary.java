package io.flowforge.application.execution;

import io.flowforge.domain.execution.WorkflowRunStatus;

import java.time.Instant;
import java.util.UUID;

public record ExecutionSummary(
        UUID id,
        UUID workflowId,
        int workflowVersion,
        WorkflowRunStatus status,
        long stateVersion,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt
) {}
