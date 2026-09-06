package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.domain.execution.ExecutionEvent;
import io.flowforge.domain.execution.TaskAttempt;
import io.flowforge.domain.execution.TaskAttemptStatus;
import io.flowforge.domain.execution.TaskRun;
import io.flowforge.domain.execution.TaskRunStatus;
import io.flowforge.domain.execution.WorkflowExecution;
import io.flowforge.domain.execution.WorkflowRunStatus;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record WorkflowExecutionResponse(
        UUID id,
        String tenantId,
        UUID workflowId,
        int workflowVersion,
        WorkflowRunStatus status,
        long stateVersion,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt,
        List<TaskResponse> tasks,
        List<AttemptResponse> attempts,
        List<EventResponse> events
) {
    public static WorkflowExecutionResponse from(WorkflowExecution execution) {
        var workflow = execution.workflow();
        return new WorkflowExecutionResponse(
                workflow.id(),
                execution.tenantId().value(),
                workflow.workflowId(),
                workflow.workflowVersion(),
                workflow.status(),
                workflow.stateVersion(),
                workflow.createdAt(),
                workflow.startedAt(),
                workflow.finishedAt(),
                execution.tasks().stream().map(TaskResponse::from).toList(),
                execution.attempts().stream().map(AttemptResponse::from).toList(),
                execution.events().stream().map(EventResponse::from).toList()
        );
    }

    public record TaskResponse(
            UUID id,
            String taskKey,
            TaskRunStatus status,
            long stateVersion,
            Instant createdAt,
            Instant startedAt,
            Instant finishedAt
    ) {
        static TaskResponse from(TaskRun task) {
            return new TaskResponse(
                    task.id(), task.taskKey(), task.status(), task.stateVersion(),
                    task.createdAt(), task.startedAt(), task.finishedAt()
            );
        }
    }

    public record AttemptResponse(
            UUID id,
            UUID taskId,
            int attemptNumber,
            TaskAttemptStatus status,
            Instant startedAt,
            Instant finishedAt,
            String errorCode,
            String errorMessage
    ) {
        static AttemptResponse from(TaskAttempt attempt) {
            return new AttemptResponse(
                    attempt.id(), attempt.taskRunId(), attempt.attemptNumber(), attempt.status(),
                    attempt.startedAt(), attempt.finishedAt(), attempt.errorCode(), attempt.errorMessage()
            );
        }
    }

    public record EventResponse(
            UUID id,
            UUID taskId,
            String type,
            String fromStatus,
            String toStatus,
            Instant occurredAt
    ) {
        static EventResponse from(ExecutionEvent event) {
            return new EventResponse(
                    event.id(), event.taskRunId(), event.type().name(), event.fromStatus(),
                    event.toStatus(), event.occurredAt()
            );
        }
    }
}
