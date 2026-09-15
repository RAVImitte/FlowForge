package io.flowforge.application.execution;

import io.flowforge.application.coordination.TokenBucketDecision;
import io.flowforge.application.coordination.TokenBucketPolicy;
import io.flowforge.application.coordination.TokenBucketRateLimiter;
import io.flowforge.application.tenancy.TenantQuota;
import io.flowforge.application.tenancy.TenantQuotaPolicy;
import io.flowforge.application.tenancy.TenantQuotaProvider;
import io.flowforge.application.workflow.PageResult;
import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.domain.execution.WorkflowExecution;
import io.flowforge.domain.execution.WorkflowRunStatus;


import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;

public final class WorkflowExecutionService {
    private static final int DEFAULT_DISPATCH_BATCH = 100;

    private final ExecutionRepository repository;
    private final TaskDispatcher dispatcher;
    private final Clock clock;
    private final BiConsumer<TaskWorkItem, Throwable> completionErrorHandler;
    private final TokenBucketRateLimiter rateLimiter;
    private final TenantQuotaProvider quotaProvider;
    private final AdmissionBackpressureObserver backpressureObserver;
    private final boolean dispatchOnStart;

    public WorkflowExecutionService(
            ExecutionRepository repository,
            TaskDispatcher dispatcher,
            Clock clock,
            BiConsumer<TaskWorkItem, Throwable> completionErrorHandler,
            TokenBucketRateLimiter rateLimiter,
            TokenBucketPolicy dispatchRateLimit,
            AdmissionBackpressureObserver backpressureObserver
    ) {
        this(
                repository,
                dispatcher,
                clock,
                completionErrorHandler,
                rateLimiter,
                dispatchRateLimit,
                backpressureObserver,
                true
        );
    }

    public WorkflowExecutionService(
            ExecutionRepository repository,
            TaskDispatcher dispatcher,
            Clock clock,
            BiConsumer<TaskWorkItem, Throwable> completionErrorHandler,
            TokenBucketRateLimiter rateLimiter,
            TokenBucketPolicy dispatchRateLimit,
            AdmissionBackpressureObserver backpressureObserver,
            boolean dispatchOnStart
    ) {
        this(
                repository, dispatcher, clock, completionErrorHandler, rateLimiter,
                tenantId -> TenantQuota.inherited(tenantId, new TenantQuotaPolicy(
                        1_000_000, 1_000_000, 1_000_000, 1_000_000,
                        dispatchRateLimit, dispatchRateLimit
                )),
                backpressureObserver, dispatchOnStart
        );
    }

    public WorkflowExecutionService(
            ExecutionRepository repository,
            TaskDispatcher dispatcher,
            Clock clock,
            BiConsumer<TaskWorkItem, Throwable> completionErrorHandler,
            TokenBucketRateLimiter rateLimiter,
            TenantQuotaProvider quotaProvider,
            AdmissionBackpressureObserver backpressureObserver,
            boolean dispatchOnStart
    ) {
        this.repository = Objects.requireNonNull(repository);
        this.dispatcher = Objects.requireNonNull(dispatcher);
        this.clock = Objects.requireNonNull(clock);
        this.completionErrorHandler = Objects.requireNonNull(completionErrorHandler);
        this.rateLimiter = Objects.requireNonNull(rateLimiter);
        this.quotaProvider = Objects.requireNonNull(quotaProvider);
        this.backpressureObserver = Objects.requireNonNull(backpressureObserver);
        this.dispatchOnStart = dispatchOnStart;
    }

    public WorkflowExecutionService(
            ExecutionRepository repository,
            TaskDispatcher dispatcher,
            Clock clock,
            BiConsumer<TaskWorkItem, Throwable> completionErrorHandler
    ) {
        this(
                repository,
                dispatcher,
                clock,
                completionErrorHandler,
                (tenantId, key, policy, requested, now) -> new TokenBucketDecision(requested, Duration.ZERO),
                new TokenBucketPolicy(1_000, 1_000, Duration.ofSeconds(1)),
                new AdmissionBackpressureObserver() { }
        );
    }

    public WorkflowExecution start(TenantId tenantId, UUID workflowId, String idempotencyKey) {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(workflowId, "workflowId must not be null");
        String normalizedKey = normalizeIdempotencyKey(idempotencyKey);
        WorkflowExecution execution = repository.start(tenantId, workflowId, normalizedKey, clock.instant());
        if (!dispatchOnStart) return execution;
        dispatchReadyTasks(tenantId, DEFAULT_DISPATCH_BATCH);
        return repository.findById(tenantId, execution.workflow().id()).orElse(execution);
    }

    public WorkflowExecution get(TenantId tenantId, UUID executionId) {
        return repository.findById(Objects.requireNonNull(tenantId), executionId)
                .orElseThrow(() -> new ExecutionNotFoundException(executionId));
    }

    public WorkflowExecution cancel(TenantId tenantId, UUID executionId) {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(executionId, "executionId must not be null");
        return repository.cancel(tenantId, executionId, clock.instant());
    }

    public PageResult<ExecutionSummary> list(TenantId tenantId, int page, int size, WorkflowRunStatus statusFilter) {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        if (page < 0) throw new IllegalArgumentException("page must be at least 0");
        if (size < 1 || size > 100) throw new IllegalArgumentException("size must be between 1 and 100");
        return repository.list(tenantId, page, size, statusFilter);
    }


    public int dispatchReadyTasks(int limit) {
        if (limit < 1 || limit > 1_000) {
            throw new IllegalArgumentException("dispatch limit must be between 1 and 1000");
        }
        Instant now = clock.instant();
        int dispatched = 0;
        for (TenantId tenantId : repository.readyTenants(now, limit)) {
            dispatched += dispatchReadyTasks(tenantId, limit - dispatched, now);
            if (dispatched >= limit) break;
        }
        return dispatched;
    }

    public int dispatchReadyTasks(TenantId tenantId, int limit) {
        if (limit < 1 || limit > 1_000) {
            throw new IllegalArgumentException("dispatch limit must be between 1 and 1000");
        }
        return dispatchReadyTasks(Objects.requireNonNull(tenantId), limit, clock.instant());
    }

    private int dispatchReadyTasks(TenantId tenantId, int limit, Instant now) {
        ReadyQueueSnapshot queue = repository.readyQueue(tenantId, now, limit);
        if (queue == null) queue = ReadyQueueSnapshot.unknown(limit);
        backpressureObserver.readyQueueObserved(queue.depth(), queue.oldestAge());
        int requested = (int) Math.min(limit, queue.depth());
        if (requested == 0) return 0;
        TokenBucketDecision decision = rateLimiter.consume(
                tenantId, "task-dispatch",
                quotaProvider.quotaFor(tenantId).policy().dispatchRateLimit(), requested, now
        );
        if (decision.throttled(requested)) {
            backpressureObserver.taskDispatchThrottled(
                    requested, decision.granted(), decision.retryAfter()
            );
        }
        if (decision.granted() == 0) return 0;
        var workItems = repository.claimReadyTasks(tenantId, decision.granted(), now);
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
