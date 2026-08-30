package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.workflow.PageResult;
import io.flowforge.domain.workflow.WorkflowDefinition;

import java.util.List;

public record WorkflowPageResponse(
        List<WorkflowResponse> items,
        int page,
        int size,
        long totalElements,
        int totalPages
) {
    static WorkflowPageResponse from(PageResult<WorkflowDefinition> result) {
        int totalPages = result.totalElements() == 0
                ? 0
                : (int) Math.ceil((double) result.totalElements() / result.size());
        return new WorkflowPageResponse(
                result.items().stream().map(WorkflowResponse::from).toList(),
                result.page(),
                result.size(),
                result.totalElements(),
                totalPages
        );
    }
}
