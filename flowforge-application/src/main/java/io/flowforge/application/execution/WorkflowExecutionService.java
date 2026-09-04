package io.flowforge.application.execution;

import io.flowforge.domain.execution.WorkflowExecution;

import java.time.Clock;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;

public final class WorkflowExecutionService {
    private static final int DEFAULT_DISPATCH_BATCH = 100;

    private final ExecutionRepository repository;
    private final TaskDispatcher dispatcher;
    private final Clock clock;
    private final BiConsumer<TaskWorkItem, Throwable> completionErrorHandler;

    public WorkflowExecutionService(
            ExecutionRepository repository,
            TaskDispatcher dispatcher,
            Clock clock,
            BiConsumer<TaskWorkItem, Throwable> completionErrorHandler
    ) {
        this.repository = Objects.requireNonNull(repository);
        this.dispatcher = Objects.requireNonNull(dispatcher);
        this.clock = Objects.requireNonNull(clock);
        this.completionErrorHandler = Objects.requireNonNull(completionErrorHandler);
    }

    public WorkflowExecution start(UUID workflowId, String idempotencyKey) {
        Objects.requireNonNull(workflowId, "workflowId must not be null");
        String normalizedKey = normalizeIdempotencyKey(idempotencyKey);
        WorkflowExecution execution = repository.start(workflowId, normalizedKey, clock.instant());
        dispatchReadyTasks(DEFAULT_DISPATCH_BATCH);
        return repository.findById(execution.workflow().id()).orElse(execution);
    }

    public WorkflowExecution get(UUID executionId) {
        return repository.findById(executionId)
                .orElseThrow(() -> new ExecutionNotFoundException(executionId));
    }

    public WorkflowExecution cancel(UUID executionId) {
        Objects.requireNonNull(executionId, "executionId must not be null");
        return repository.cancel(executionId, clock.instant());
    }

    public int dispatchReadyTasks(int limit) {
        if (limit < 1 || limit > 1_000) {
            throw new IllegalArgumentException("dispatch limit must be between 1 and 1000");
        }
        var workItems = repository.claimReadyTasks(limit, clock.instant());
        workItems.forEach(this::dispatch);
        return workItems.size();
    }

    private void dispatch(TaskWorkItem workItem) {
        try {
            dispatcher.dispatch(workItem).whenComplete((result, failure) -> {
                if (failure != null) {
                    complete(workItem, TaskResult.failed("DISPATCH_FAILURE", rootMessage(failure)));
                } else if (result == null) {
                    complete(workItem, TaskResult.failed("EMPTY_RESULT", "Task handler returned no result"));
                } else {
                    complete(workItem, result);
                }
            });
        } catch (RuntimeException failure) {
            complete(workItem, TaskResult.failed("DISPATCH_FAILURE", rootMessage(failure)));
        }
    }

    private void complete(TaskWorkItem workItem, TaskResult result) {
        try {
            repository.completeTask(TaskCompletion.from(workItem, result), clock.instant());
        } catch (RuntimeException failure) {
            completionErrorHandler.accept(workItem, failure);
        }
    }

    private static String normalizeIdempotencyKey(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Idempotency-Key must not be blank");
        }
        String normalized = value.strip();
        if (normalized.length() > 200) {
            throw new IllegalArgumentException("Idempotency-Key must not exceed 200 characters");
        }
        return normalized;
    }

    private static String rootMessage(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null) current = current.getCause();
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }
}
