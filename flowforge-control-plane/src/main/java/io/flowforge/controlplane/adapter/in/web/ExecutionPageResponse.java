package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.execution.ExecutionSummary;
import io.flowforge.application.workflow.PageResult;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record ExecutionPageResponse(
        List<Item> items,
        int page,
        int size,
        long totalElements,
        int totalPages
) {
    public static ExecutionPageResponse from(PageResult<ExecutionSummary> result) {
        int totalPages = result.totalElements() == 0
                ? 0
                : (int) Math.ceil((double) result.totalElements() / result.size());
        return new ExecutionPageResponse(
                result.items().stream().map(Item::from).toList(),
                result.page(),
                result.size(),
                result.totalElements(),
                totalPages
        );
    }

    public record Item(
            UUID id,
            UUID workflowId,
            int workflowVersion,
            String status,
            long stateVersion,
            Instant createdAt,
            Instant startedAt,
            Instant finishedAt
    ) {
        static Item from(ExecutionSummary summary) {
            return new Item(
                    summary.id(),
                    summary.workflowId(),
                    summary.workflowVersion(),
                    summary.status().name(),
                    summary.stateVersion(),
                    summary.createdAt(),
                    summary.startedAt(),
                    summary.finishedAt()
            );
        }
    }
}
