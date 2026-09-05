package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.workflow.PageResult;
import io.flowforge.domain.schedule.WorkflowSchedule;

import java.util.List;

public record SchedulePageResponse(
        List<ScheduleResponse> items,
        int page,
        int size,
        long totalElements
) {
    static SchedulePageResponse from(PageResult<WorkflowSchedule> result) {
        return new SchedulePageResponse(
                result.items().stream().map(ScheduleResponse::from).toList(),
                result.page(),
                result.size(),
                result.totalElements()
        );
    }
}
