package io.flowforge.controlplane.adapter.out.persistence;

import io.flowforge.application.execution.ExecutionConflictException;
import io.flowforge.application.execution.ConcurrencyLimitExceededException;
import io.flowforge.application.execution.ConcurrencyLifecycleObserver;
import io.flowforge.application.execution.ExecutionNotFoundException;
import io.flowforge.application.execution.AttemptTimeoutRecovery;
import io.flowforge.application.execution.AttemptLeaseRecovery;
import io.flowforge.application.execution.DurableTaskQueue;
import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.execution.RetryLifecycleObserver;
import io.flowforge.application.execution.TaskCompletion;
import io.flowforge.application.execution.TaskCompletionResult;
import io.flowforge.application.execution.TaskOutcome;
import io.flowforge.application.execution.TaskWorkItem;
import io.flowforge.application.execution.TimeoutLifecycleObserver;
import io.flowforge.application.execution.WorkflowNotPublishedException;
import io.flowforge.application.coordination.CoordinationPermit;
import io.flowforge.application.coordination.CoordinationPermitLedger;
import io.flowforge.application.coordination.CoordinationPermitService;
import io.flowforge.domain.execution.DagResolver;
import io.flowforge.domain.execution.ExecutionEvent;
import io.flowforge.domain.execution.ExecutionEventType;
import io.flowforge.domain.execution.RetryBackoff;
import io.flowforge.domain.execution.TaskAttempt;
import io.flowforge.domain.execution.TaskAttemptStatus;
import io.flowforge.domain.execution.TaskRun;
import io.flowforge.domain.execution.TaskRunStatus;
import io.flowforge.domain.execution.WorkflowExecution;
import io.flowforge.domain.execution.WorkflowRun;
import io.flowforge.domain.execution.WorkflowRunStatus;
import io.flowforge.domain.workflow.TaskDependency;
import io.flowforge.domain.workflow.TaskReliabilityPolicy;
import io.flowforge.messaging.ExecutionEventV1;
import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskCommandV1;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Repository
public class JdbcExecutionRepository implements ExecutionRepository, DurableTaskQueue, AttemptTimeoutRecovery,
        AttemptLeaseRecovery {
    private final JdbcClient jdbc;
    private final ObjectMapper objectMapper;
    private final RetryLifecycleObserver retryObserver;
    private final TimeoutLifecycleObserver timeoutObserver;
    private final Duration workerLeaseDuration;
    private final Duration concurrencyLeaseDuration;
    private final CoordinationPermitLedger permitLedger;
    private final ObjectProvider<CoordinationPermitService> permitService;
    private final ConcurrencyLifecycleObserver concurrencyObserver;

    public JdbcExecutionRepository(
            JdbcClient jdbc,
            ObjectMapper objectMapper,
            RetryLifecycleObserver retryObserver,
            TimeoutLifecycleObserver timeoutObserver,
            CoordinationPermitLedger permitLedger,
            ObjectProvider<CoordinationPermitService> permitService,
            ConcurrencyLifecycleObserver concurrencyObserver,
            @Value("${flowforge.leases.duration:30s}") Duration workerLeaseDuration,
            @Value("${flowforge.concurrency.lease-duration:30s}") Duration concurrencyLeaseDuration
    ) {
        if (workerLeaseDuration == null || workerLeaseDuration.isZero() || workerLeaseDuration.isNegative()) {
            throw new IllegalArgumentException("Worker lease duration must be positive");
        }
        if (concurrencyLeaseDuration == null
                || concurrencyLeaseDuration.isZero()
                || concurrencyLeaseDuration.isNegative()) {
            throw new IllegalArgumentException("Concurrency lease duration must be positive");
        }
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.retryObserver = retryObserver;
        this.timeoutObserver = timeoutObserver;
        this.workerLeaseDuration = workerLeaseDuration;
        this.concurrencyLeaseDuration = concurrencyLeaseDuration;
        this.permitLedger = permitLedger;
        this.permitService = permitService;
        this.concurrencyObserver = concurrencyObserver;
    }

    @Override
    @Transactional
    public WorkflowExecution start(UUID workflowId, String idempotencyKey, Instant now) {
        Optional<UUID> existing = findByIdempotencyKey(workflowId, idempotencyKey);
        if (existing.isPresent()) return get(existing.get());

        PublishedVersion version = jdbc.sql("""
                SELECT v.id, v.version_number, v.max_concurrent_executions
                  FROM workflow_version v
                  JOIN workflow_definition w ON w.id = v.workflow_id
                 WHERE v.workflow_id = :workflowId
                   AND v.version_status = 'PUBLISHED'
                   AND w.lifecycle_status = 'ACTIVE'
                 ORDER BY v.version_number DESC
                 LIMIT 1
                 FOR SHARE OF v, w
                """)
                .param("workflowId", workflowId)
                .query((rs, rowNum) -> new PublishedVersion(
                        rs.getObject("id", UUID.class),
                        rs.getInt("version_number"),
                        (Integer) rs.getObject("max_concurrent_executions")
                ))
                .optional()
                .orElseThrow(() -> new WorkflowNotPublishedException(workflowId));

        UUID executionId = UUID.randomUUID();
        UUID concurrencyPermitToken = null;
        if (version.maxConcurrentExecutions() != null) {
            String resourceKey = workflowResource(version.id());
            Optional<CoordinationPermit> acquired = permitLedger.tryAcquire(
                    resourceKey,
                    executionId.toString(),
                    UUID.randomUUID(),
                    version.maxConcurrentExecutions(),
                    now,
                    concurrencyLeaseDuration
            );
            if (acquired.isEmpty()) {
                Optional<UUID> concurrentReplay = findByIdempotencyKey(workflowId, idempotencyKey);
                if (concurrentReplay.isPresent()) return get(concurrentReplay.get());
                concurrencyObserver.workflowRejected();
                throw new ConcurrencyLimitExceededException(workflowId, version.maxConcurrentExecutions());
            }
            concurrencyPermitToken = acquired.get().token();
            long activeExecutions = jdbc.sql("""
                    SELECT COUNT(*)
                      FROM workflow_execution
                     WHERE workflow_version_id = :versionId
                       AND status IN ('PENDING', 'RUNNING', 'CANCELLING')
                    """)
                    .param("versionId", version.id())
                    .query(Long.class)
                    .single();
            if (activeExecutions >= version.maxConcurrentExecutions()) {
                releasePermit(concurrencyPermitToken, now);
                Optional<UUID> concurrentReplay = findByIdempotencyKey(workflowId, idempotencyKey);
                if (concurrentReplay.isPresent()) return get(concurrentReplay.get());
                concurrencyObserver.workflowRejected();
                throw new ConcurrencyLimitExceededException(workflowId, version.maxConcurrentExecutions());
            }
            scheduleReconciliation(resourceKey);
        }
        WorkflowRun running = WorkflowRun.pending(executionId, workflowId, version.versionNumber(), now)
                .transitionTo(WorkflowRunStatus.RUNNING, now);
        int inserted = jdbc.sql("""
                INSERT INTO workflow_execution(
                    id, workflow_id, workflow_version_id, workflow_version_number,
                    idempotency_key, status, state_version, created_at, started_at, finished_at,
                    concurrency_permit_token
                ) VALUES (
                    :id, :workflowId, :workflowVersionId, :workflowVersionNumber,
                    :idempotencyKey, :status, :stateVersion, :createdAt, :startedAt, :finishedAt,
                    :concurrencyPermitToken
                )
                ON CONFLICT (workflow_id, idempotency_key) DO NOTHING
                """)
                .param("id", running.id())
                .param("workflowId", running.workflowId())
                .param("workflowVersionId", version.id())
                .param("workflowVersionNumber", running.workflowVersion())
                .param("idempotencyKey", idempotencyKey)
                .param("status", running.status().name())
                .param("stateVersion", running.stateVersion())
                .param("createdAt", timestamp(running.createdAt()))
                .param("startedAt", timestamp(running.startedAt()))
                .param("finishedAt", timestamp(running.finishedAt()))
                .param("concurrencyPermitToken", concurrencyPermitToken)
                .update();
        if (inserted == 0) {
            releasePermit(concurrencyPermitToken, now);
            UUID existingId = findByIdempotencyKey(workflowId, idempotencyKey).orElseThrow();
            return get(existingId);
        }
        if (concurrencyPermitToken != null) concurrencyObserver.acquired("workflow");

        insertEvent(executionId, null, ExecutionEventType.WORKFLOW_CREATED, null, "PENDING", now);
        insertEvent(executionId, null, ExecutionEventType.WORKFLOW_STARTED, "PENDING", "RUNNING", now);
        materializeTasks(executionId, version.id(), now);
        return get(executionId);
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<WorkflowExecution> findById(UUID executionId) {
        return findWorkflow(executionId).map(workflow -> snapshot(workflow));
    }

    @Override
    @Transactional
    public WorkflowExecution cancel(UUID executionId, Instant now) {
        WorkflowRun workflow = lockWorkflow(executionId);
        if (workflow.status().isTerminal()) return snapshot(workflow);

        WorkflowRun cancelling;
        if (workflow.status() == WorkflowRunStatus.PENDING) {
            cancelling = workflow.transitionTo(WorkflowRunStatus.CANCELLED, now);
            updateWorkflow(workflow, cancelling);
            insertEvent(executionId, null, ExecutionEventType.WORKFLOW_CANCELLED,
                    workflow.status().name(), cancelling.status().name(), now);
            releaseWorkflowPermit(executionId, now);
            return snapshot(cancelling);
        }
        if (workflow.status() == WorkflowRunStatus.RUNNING) {
            cancelling = workflow.transitionTo(WorkflowRunStatus.CANCELLING, now);
            updateWorkflow(workflow, cancelling);
            insertEvent(executionId, null, ExecutionEventType.WORKFLOW_CANCELLATION_REQUESTED,
                    workflow.status().name(), cancelling.status().name(), now);
        } else {
            cancelling = workflow;
        }

        cancelQueuedTasks(executionId, now);
        if (countTasksWithStatus(executionId, TaskRunStatus.RUNNING) == 0) {
            WorkflowRun cancelled = cancelling.transitionTo(WorkflowRunStatus.CANCELLED, now);
            updateWorkflow(cancelling, cancelled);
            insertEvent(executionId, null, ExecutionEventType.WORKFLOW_CANCELLED,
                    cancelling.status().name(), cancelled.status().name(), now);
            releaseWorkflowPermit(executionId, now);
            cancelling = cancelled;
        }
        return snapshot(cancelling);
    }

    @Override
    @Transactional
    public List<TaskWorkItem> claimReadyTasks(int limit, Instant now) {
        return claimCandidates(limit, null).stream()
                .map(candidate -> claim(candidate, now, false))
                .flatMap(Optional::stream)
                .toList();
    }

    @Override
    @Transactional
    public int enqueueReadyTasks(int limit, Instant now) {
        List<ClaimCandidate> candidates = claimCandidates(limit, null);
        return (int) candidates.stream()
                .map(candidate -> claim(candidate, now, true))
                .flatMap(Optional::stream)
                .count();
    }

    @Override
    @Transactional
    public int enqueueReadyTasks(UUID workflowExecutionId, int limit, Instant now) {
        List<ClaimCandidate> candidates = claimCandidates(limit, workflowExecutionId);
        return (int) candidates.stream()
                .map(candidate -> claim(candidate, now, true))
                .flatMap(Optional::stream)
                .count();
    }

    @Override
    @Transactional
    public int releaseDueRetries(int limit, Instant now) {
        List<TaskRun> due = jdbc.sql("""
                SELECT te.id, te.workflow_execution_id, te.task_key, te.status, te.state_version,
                       te.created_at, te.started_at, te.finished_at, te.next_attempt_at
                  FROM task_execution te
                  JOIN workflow_execution we ON we.id = te.workflow_execution_id
                 WHERE te.status = 'RETRY_SCHEDULED'
                   AND te.next_attempt_at <= :now
                   AND we.status = 'RUNNING'
                 ORDER BY te.next_attempt_at, te.id
                 FOR UPDATE OF we, te SKIP LOCKED
                 LIMIT :limit
                """)
                .param("now", timestamp(now))
                .param("limit", limit)
                .query((rs, rowNum) -> mapTaskRun(rs))
                .list();
        due.forEach(task -> {
            TaskRun ready = task.transitionTo(TaskRunStatus.READY, now);
            updateTask(task, ready);
            insertEvent(
                    task.workflowRunId(),
                    task.id(),
                    ExecutionEventType.TASK_RETRY_READY,
                    task.status().name(),
                    ready.status().name(),
                    now
            );
        });
        return due.size();
    }

    @Override
    @Transactional
    public int reapTimedOutAttempts(int limit, Instant now) {
        List<UUID> dueTaskIds = jdbc.sql("""
                SELECT te.id
                  FROM workflow_execution we
                  JOIN task_execution te ON te.workflow_execution_id = we.id
                  JOIN task_attempt ta
                    ON ta.task_execution_id = te.id
                   AND ta.status = 'RUNNING'
                 WHERE we.status = 'RUNNING'
                   AND te.status = 'RUNNING'
                   AND ta.attempt_deadline IS NOT NULL
                   AND ta.attempt_deadline <= :now
                 ORDER BY ta.attempt_deadline, te.id
                 FOR UPDATE OF we, te, ta SKIP LOCKED
                 LIMIT :limit
                """)
                .param("now", timestamp(now))
                .param("limit", limit)
                .query(UUID.class)
                .list();

        dueTaskIds.forEach(taskId -> {
            WorkflowRun workflow = lockWorkflowForTask(taskId);
            TaskRun task = lockTask(taskId);
            completeRunningTask(
                    workflow,
                    task,
                    new TaskCompletion(
                            task.id(),
                            task.stateVersion(),
                            TaskOutcome.TIMED_OUT,
                            "TASK_TIMEOUT",
                            "Attempt deadline exceeded",
                            true
                    ),
                    now
            );
        });
        return dueTaskIds.size();
    }

    @Override
    @Transactional
    public int reapExpiredLeases(int limit, Instant now) {
        List<LeaseCandidate> dueAttempts = jdbc.sql("""
                SELECT te.id, ta.fencing_token
                  FROM workflow_execution we
                  JOIN task_execution te ON te.workflow_execution_id = we.id
                  JOIN task_attempt ta
                    ON ta.task_execution_id = te.id
                   AND ta.status = 'RUNNING'
                 WHERE we.status = 'RUNNING'
                   AND te.status = 'RUNNING'
                   AND ta.lease_deadline IS NOT NULL
                   AND ta.lease_deadline <= :now
                   AND (ta.attempt_deadline IS NULL OR ta.attempt_deadline > :now)
                 ORDER BY ta.lease_deadline, te.id
                 FOR UPDATE OF we, te, ta SKIP LOCKED
                 LIMIT :limit
                """)
                .param("now", timestamp(now))
                .param("limit", limit)
                .query((rs, rowNum) -> new LeaseCandidate(
                        rs.getObject("id", UUID.class),
                        rs.getObject("fencing_token", UUID.class)
                ))
                .list();

        dueAttempts.forEach(candidate -> {
            WorkflowRun workflow = lockWorkflowForTask(candidate.taskId());
            TaskRun task = lockTask(candidate.taskId());
            completeRunningTask(
                    workflow,
                    task,
                    new TaskCompletion(
                            task.id(),
                            task.stateVersion(),
                            TaskOutcome.FAILED,
                            "WORKER_LEASE_EXPIRED",
                            "Worker stopped renewing the attempt lease",
                            true,
                            candidate.fencingToken()
                    ),
                    now
            );
        });
        return dueAttempts.size();
    }

    private List<ClaimCandidate> claimCandidates(int limit, UUID workflowExecutionId) {
        String workflowFilter = workflowExecutionId == null
                ? ""
                : " AND te.workflow_execution_id = :workflowExecutionId\n";
        var query = jdbc.sql("""
                SELECT te.id, te.workflow_execution_id, te.task_key, te.status, te.state_version,
                       te.created_at, te.started_at, te.finished_at, te.next_attempt_at,
                       wt.task_type, wt.configuration, wt.attempt_timeout_ms, wt.max_concurrency,
                       we.workflow_version_id,
                       COALESCE((SELECT MAX(ta.attempt_number)
                                   FROM task_attempt ta
                                  WHERE ta.task_execution_id = te.id), 0) + 1 AS attempt_number
                  FROM task_execution te
                  JOIN workflow_execution we ON we.id = te.workflow_execution_id
                  JOIN workflow_task wt
                    ON wt.workflow_version_id = we.workflow_version_id
                   AND wt.task_key = te.task_key
                 WHERE te.status = 'READY'
                   AND we.status = 'RUNNING'
                """ + workflowFilter + """
                 ORDER BY te.created_at, te.id
                 FOR UPDATE OF we, te SKIP LOCKED
                 LIMIT :limit
                """)
                .param("limit", limit);
        if (workflowExecutionId != null) query = query.param("workflowExecutionId", workflowExecutionId);
        return query.query((rs, rowNum) -> new ClaimCandidate(
                        mapTaskRun(rs),
                        rs.getString("task_type"),
                        fromJson(rs.getString("configuration")),
                        rs.getInt("attempt_number"),
                        rs.getObject("attempt_timeout_ms") == null
                                ? null
                                : rs.getLong("attempt_timeout_ms"),
                        rs.getObject("workflow_version_id", UUID.class),
                        (Integer) rs.getObject("max_concurrency")
                ))
                .list();
    }

    @Override
    @Transactional
    public TaskCompletionResult completeTask(TaskCompletion completion, Instant now) {
        WorkflowRun workflow = lockWorkflowForTask(completion.taskRunId());
        TaskRun current = lockTask(completion.taskRunId());
        TaskRunStatus target = completion.outcome().toTaskStatus();

        if (current.status().isTerminal()) {
            if (current.status() == target) {
                return new TaskCompletionResult(snapshot(workflow), false);
            }
            throw new ExecutionConflictException(
                    "Task " + current.id() + " already completed as " + current.status()
            );
        }
        if (current.stateVersion() != completion.expectedStateVersion()) {
            throw new ExecutionConflictException(
                    "Task " + current.id() + " was modified concurrently; expected state version "
                            + completion.expectedStateVersion() + " but found " + current.stateVersion()
            );
        }

        if (current.status() != TaskRunStatus.RUNNING) {
            throw new ExecutionConflictException("Task " + current.id() + " has no active running attempt");
        }

        WorkflowRun resultingWorkflow = completeRunningTask(workflow, current, completion, now);
        return new TaskCompletionResult(snapshot(resultingWorkflow), true);
    }

    private WorkflowRun completeRunningTask(
            WorkflowRun workflow,
            TaskRun current,
            TaskCompletion completion,
            Instant now
    ) {
        TaskRunStatus target = completion.outcome().toTaskStatus();
        RetryContext retry = loadRetryContext(current.id());
        if (completion.fencingToken() != null && !completion.fencingToken().equals(retry.fencingToken())) {
            throw new ExecutionConflictException("Stale fencing token for task " + current.id());
        }
        boolean retryableFailure = (completion.outcome() == TaskOutcome.FAILED
                || completion.outcome() == TaskOutcome.TIMED_OUT)
                && retry.policy().isRetryable(completion.errorCode(), completion.retryable());
        if (retryableFailure && retry.attemptNumber() < retry.policy().maxAttempts()) {
            Duration delay = RetryBackoff.delayAfter(retry.policy(), current.id(), retry.attemptNumber());
            Instant retryAt = now.plus(delay);
            TaskRun scheduled = current.scheduleRetry(now, retryAt);
            updateTask(current, scheduled);
            completeAttempt(scheduled, completion, now);
            insertAttemptTimeoutEventIfNeeded(workflow.id(), current, completion, now);
            insertAttemptLeaseExpiredEventIfNeeded(workflow.id(), current, completion, now);
            insertEvent(
                    workflow.id(),
                    scheduled.id(),
                    ExecutionEventType.TASK_RETRY_SCHEDULED,
                    current.status().name(),
                    scheduled.status().name(),
                    now
            );
            retryObserver.scheduled(delay);
            if (completion.outcome() == TaskOutcome.TIMED_OUT) {
                timeoutObserver.attemptTimedOut(true, false);
            }
            return workflow;
        }

        TaskRun completed = current.transitionTo(target, now);
        updateTask(current, completed);
        completeAttempt(completed, completion, now);
        insertAttemptTimeoutEventIfNeeded(workflow.id(), current, completion, now);
        insertAttemptLeaseExpiredEventIfNeeded(workflow.id(), current, completion, now);
        insertEvent(
                workflow.id(),
                completed.id(),
                taskEvent(target),
                current.status().name(),
                completed.status().name(),
                now
        );
        if (retryableFailure && retry.policy().maxAttempts() > 1) {
            insertEvent(
                    workflow.id(),
                    completed.id(),
                    ExecutionEventType.TASK_RETRY_EXHAUSTED,
                    current.status().name(),
                    completed.status().name(),
                    now
            );
            insertEvent(
                    workflow.id(),
                    completed.id(),
                    ExecutionEventType.TASK_DEAD_LETTERED,
                    current.status().name(),
                    completed.status().name(),
                    now
            );
            retryObserver.exhausted();
            retryObserver.deadLettered();
        }

        WorkflowRun resultingWorkflow = advanceWorkflow(workflow, completed, now);
        if (completion.outcome() == TaskOutcome.TIMED_OUT) {
            timeoutObserver.attemptTimedOut(
                    false,
                    workflow.status() != WorkflowRunStatus.FAILED
                            && resultingWorkflow.status() == WorkflowRunStatus.FAILED
            );
        }
        return resultingWorkflow;
    }

    private void insertAttemptTimeoutEventIfNeeded(
            UUID workflowId,
            TaskRun current,
            TaskCompletion completion,
            Instant now
    ) {
        if (completion.outcome() == TaskOutcome.TIMED_OUT) {
            insertEvent(
                    workflowId,
                    current.id(),
                    ExecutionEventType.TASK_ATTEMPT_TIMED_OUT,
                    current.status().name(),
                    current.status().name(),
                    now
            );
        }
    }

    private void insertAttemptLeaseExpiredEventIfNeeded(
            UUID workflowId,
            TaskRun current,
            TaskCompletion completion,
            Instant now
    ) {
        if ("WORKER_LEASE_EXPIRED".equals(completion.errorCode())) {
            insertEvent(
                    workflowId,
                    current.id(),
                    ExecutionEventType.TASK_LEASE_EXPIRED,
                    current.status().name(),
                    current.status().name(),
                    now
            );
        }
    }

    private void materializeTasks(UUID executionId, UUID versionId, Instant now) {
        List<TaskTemplate> tasks = jdbc.sql("""
                SELECT task_key, task_type, configuration
                  FROM workflow_task
                 WHERE workflow_version_id = :versionId
                 ORDER BY position
                """)
                .param("versionId", versionId)
                .query((rs, rowNum) -> new TaskTemplate(
                        rs.getString("task_key"),
                        rs.getString("task_type"),
                        fromJson(rs.getString("configuration"))
                ))
                .list();
        List<TaskDependency> dependencies = loadDependencies(versionId);
        Map<String, TaskRunStatus> statuses = DagResolver.initialTaskStatuses(
                tasks.stream().map(TaskTemplate::taskKey).toList(),
                dependencies
        );

        for (TaskTemplate task : tasks) {
            UUID taskId = UUID.randomUUID();
            TaskRunStatus status = statuses.get(task.taskKey());
            jdbc.sql("""
                    INSERT INTO task_execution(
                        id, workflow_execution_id, task_key, status, state_version,
                        created_at, started_at, finished_at
                    ) VALUES (
                        :id, :executionId, :taskKey, :status, 0, :createdAt, NULL, NULL
                    )
                    """)
                    .param("id", taskId)
                    .param("executionId", executionId)
                    .param("taskKey", task.taskKey())
                    .param("status", status.name())
                    .param("createdAt", timestamp(now))
                    .update();
            if (status == TaskRunStatus.READY) {
                insertEvent(executionId, taskId, ExecutionEventType.TASK_READY, "BLOCKED", "READY", now);
            }
        }
    }

    private Optional<TaskWorkItem> claim(ClaimCandidate candidate, Instant now, boolean enqueueCommand) {
        TaskRun current = candidate.task();
        UUID attemptId = UUID.randomUUID();
        UUID concurrencyPermitToken = acquireTaskPermit(candidate, attemptId, now);
        if (candidate.maxConcurrency() != null && concurrencyPermitToken == null) {
            concurrencyObserver.taskDeferred();
            return Optional.empty();
        }
        TaskRun running = current.transitionTo(TaskRunStatus.RUNNING, now);
        updateTask(current, running);
        UUID fencingToken = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO task_attempt(
                    id, task_execution_id, attempt_number, status, started_at, attempt_deadline,
                    lease_deadline, fencing_token, concurrency_permit_token
                ) VALUES (
                    :id, :taskId, :attemptNumber, 'RUNNING', :startedAt, :attemptDeadline,
                    :leaseDeadline, :fencingToken, :concurrencyPermitToken
                )
                """)
                .param("id", attemptId)
                .param("taskId", running.id())
                .param("attemptNumber", candidate.attemptNumber())
                .param("startedAt", timestamp(now))
                .param("attemptDeadline", candidate.attemptTimeoutMs() == null
                        ? null
                        : timestamp(now.plusMillis(candidate.attemptTimeoutMs())))
                .param("leaseDeadline", enqueueCommand ? timestamp(now.plus(workerLeaseDuration)) : null)
                .param("fencingToken", fencingToken)
                .param("concurrencyPermitToken", concurrencyPermitToken)
                .update();
        if (concurrencyPermitToken != null) concurrencyObserver.acquired("task");
        insertEvent(
                running.workflowRunId(),
                running.id(),
                candidate.attemptNumber() == 1
                        ? ExecutionEventType.TASK_STARTED
                        : ExecutionEventType.TASK_RETRY_STARTED,
                current.status().name(),
                running.status().name(),
                now
        );
        TaskWorkItem workItem = new TaskWorkItem(
                running.workflowRunId(),
                running.id(),
                running.taskKey(),
                candidate.taskType(),
                candidate.configuration(),
                running.stateVersion(),
                candidate.attemptNumber(),
                fencingToken,
                candidate.attemptTimeoutMs()
        );
        if (enqueueCommand) insertTaskCommand(workItem, now);
        if (candidate.attemptNumber() > 1) retryObserver.started();
        return Optional.of(workItem);
    }

    private UUID acquireTaskPermit(ClaimCandidate candidate, UUID attemptId, Instant now) {
        if (candidate.maxConcurrency() == null) return null;
        String resourceKey = taskResource(candidate.workflowVersionId(), candidate.task().taskKey());
        Optional<CoordinationPermit> acquired = permitLedger.tryAcquire(
                resourceKey,
                attemptId.toString(),
                UUID.randomUUID(),
                candidate.maxConcurrency(),
                now,
                concurrencyLeaseDuration
        );
        if (acquired.isEmpty()) return null;

        long runningTasks = jdbc.sql("""
                SELECT COUNT(*)
                  FROM task_execution te
                  JOIN workflow_execution we ON we.id = te.workflow_execution_id
                 WHERE we.workflow_version_id = :versionId
                   AND te.task_key = :taskKey
                   AND te.status = 'RUNNING'
                """)
                .param("versionId", candidate.workflowVersionId())
                .param("taskKey", candidate.task().taskKey())
                .query(Long.class)
                .single();
        if (runningTasks >= candidate.maxConcurrency()) {
            releasePermit(acquired.get().token(), now);
            return null;
        }
        scheduleReconciliation(resourceKey);
        return acquired.get().token();
    }

    private void releaseWorkflowPermit(UUID executionId, Instant now) {
        UUID token = jdbc.sql("""
                SELECT concurrency_permit_token
                  FROM workflow_execution
                 WHERE id = :executionId
                """)
                .param("executionId", executionId)
                .query((rs, rowNum) -> rs.getObject("concurrency_permit_token", UUID.class))
                .optional()
                .orElse(null);
        releasePermit(token, now);
    }

    private void releasePermit(UUID token, Instant now) {
        if (token == null) return;
        permitLedger.release(token, now).ifPresent(permit -> {
            String scope = permit.resourceKey().startsWith("concurrency:task:") ? "task" : "workflow";
            concurrencyObserver.released(scope);
            scheduleReconciliation(permit.resourceKey());
        });
    }

    private void scheduleReconciliation(String resourceKey) {
        CoordinationPermitService service = permitService.getIfAvailable();
        if (service == null) return;
        Runnable reconcile = () -> service.reconcile(resourceKey);
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            reconcile.run();
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                reconcile.run();
            }
        });
    }

    private static String workflowResource(UUID workflowVersionId) {
        return "concurrency:workflow-version:" + workflowVersionId;
    }

    private static String taskResource(UUID workflowVersionId, String taskKey) {
        return "concurrency:task:" + workflowVersionId + ":" + taskKey;
    }

    private WorkflowRun advanceWorkflow(WorkflowRun workflow, TaskRun completed, Instant now) {
        if (workflow.status() == WorkflowRunStatus.RUNNING) {
            if (completed.status() == TaskRunStatus.SUCCEEDED) {
                markNewlyReadyTasks(workflow.id(), now);
                if (allTasksSucceeded(workflow.id())) {
                    WorkflowRun succeeded = workflow.transitionTo(WorkflowRunStatus.SUCCEEDED, now);
                    updateWorkflow(workflow, succeeded);
                    insertEvent(workflow.id(), null, ExecutionEventType.WORKFLOW_SUCCEEDED,
                            workflow.status().name(), succeeded.status().name(), now);
                    releaseWorkflowPermit(workflow.id(), now);
                    return succeeded;
                }
            } else {
                cancelQueuedTasks(workflow.id(), now);
                WorkflowRun failed = workflow.transitionTo(WorkflowRunStatus.FAILED, now);
                updateWorkflow(workflow, failed);
                insertEvent(workflow.id(), null, ExecutionEventType.WORKFLOW_FAILED,
                        workflow.status().name(), failed.status().name(), now);
                releaseWorkflowPermit(workflow.id(), now);
                return failed;
            }
        } else if (workflow.status() == WorkflowRunStatus.CANCELLING && allTasksTerminal(workflow.id())) {
            WorkflowRun cancelled = workflow.transitionTo(WorkflowRunStatus.CANCELLED, now);
            updateWorkflow(workflow, cancelled);
            insertEvent(workflow.id(), null, ExecutionEventType.WORKFLOW_CANCELLED,
                    workflow.status().name(), cancelled.status().name(), now);
            releaseWorkflowPermit(workflow.id(), now);
            return cancelled;
        }
        return workflow;
    }

    private void markNewlyReadyTasks(UUID executionId, Instant now) {
        StoredExecution stored = storedExecution(executionId);
        List<TaskRun> tasks = loadTasks(executionId);
        Map<String, TaskRunStatus> statuses = new LinkedHashMap<>();
        tasks.forEach(task -> statuses.put(task.taskKey(), task.status()));
        List<TaskDependency> dependencies = loadDependencies(stored.workflowVersionId());

        for (String taskKey : DagResolver.newlyReadyTasks(statuses, dependencies)) {
            TaskRun blocked = tasks.stream()
                    .filter(task -> task.taskKey().equals(taskKey))
                    .findFirst()
                    .orElseThrow();
            TaskRun ready = blocked.transitionTo(TaskRunStatus.READY, now);
            updateTask(blocked, ready);
            insertEvent(executionId, ready.id(), ExecutionEventType.TASK_READY,
                    blocked.status().name(), ready.status().name(), now);
        }
    }

    private void cancelQueuedTasks(UUID executionId, Instant now) {
        List<TaskRun> queued = jdbc.sql("""
                SELECT id, workflow_execution_id, task_key, status, state_version,
                       created_at, started_at, finished_at, next_attempt_at
                  FROM task_execution
                 WHERE workflow_execution_id = :executionId
                   AND status IN ('BLOCKED', 'READY', 'RETRY_SCHEDULED')
                 ORDER BY task_key
                 FOR UPDATE
                """)
                .param("executionId", executionId)
                .query((rs, rowNum) -> mapTaskRun(rs))
                .list();
        queued.forEach(task -> {
            TaskRun cancelled = task.transitionTo(TaskRunStatus.CANCELLED, now);
            updateTask(task, cancelled);
            insertEvent(executionId, task.id(), ExecutionEventType.TASK_CANCELLED,
                    task.status().name(), cancelled.status().name(), now);
        });
    }

    private void completeAttempt(TaskRun completed, TaskCompletion completion, Instant now) {
        Optional<CompletedAttempt> attempt = jdbc.sql("""
                UPDATE task_attempt
                   SET status = :status,
                       finished_at = :finishedAt,
                       error_code = :errorCode,
                       error_message = :errorMessage
                 WHERE task_execution_id = :taskId
                   AND status = 'RUNNING'
                RETURNING concurrency_permit_token
                """)
                .param("status", TaskAttemptStatus.valueOf(completion.outcome().name()).name())
                .param("finishedAt", timestamp(now))
                .param("errorCode", truncate(completion.errorCode(), 100))
                .param("errorMessage", truncate(completion.errorMessage(), 2_000))
                .param("taskId", completed.id())
                .query((rs, rowNum) -> new CompletedAttempt(
                        rs.getObject("concurrency_permit_token", UUID.class)
                ))
                .optional();
        if (attempt.isEmpty()) {
            throw new ExecutionConflictException("No running attempt exists for task " + completed.id());
        }
        releasePermit(attempt.get().concurrencyPermitToken(), now);
    }

    private RetryContext loadRetryContext(UUID taskId) {
        return jdbc.sql("""
                SELECT ta.attempt_number, ta.fencing_token,
                       wt.max_attempts, wt.initial_backoff_ms, wt.backoff_multiplier,
                       wt.max_backoff_ms, wt.jitter_factor, wt.attempt_timeout_ms,
                       wt.retryable_error_codes
                  FROM task_execution te
                  JOIN workflow_execution we ON we.id = te.workflow_execution_id
                  JOIN workflow_task wt
                    ON wt.workflow_version_id = we.workflow_version_id
                   AND wt.task_key = te.task_key
                  JOIN task_attempt ta
                    ON ta.task_execution_id = te.id
                   AND ta.status = 'RUNNING'
                 WHERE te.id = :taskId
                 FOR UPDATE OF ta
                """)
                .param("taskId", taskId)
                .query((rs, rowNum) -> new RetryContext(
                        rs.getInt("attempt_number"),
                        rs.getObject("fencing_token", UUID.class),
                        new TaskReliabilityPolicy(
                                rs.getInt("max_attempts"),
                                Duration.ofMillis(rs.getLong("initial_backoff_ms")),
                                rs.getDouble("backoff_multiplier"),
                                Duration.ofMillis(rs.getLong("max_backoff_ms")),
                                rs.getDouble("jitter_factor"),
                                rs.getObject("attempt_timeout_ms") == null
                                        ? null
                                        : Duration.ofMillis(rs.getLong("attempt_timeout_ms")),
                                stringSetFromJson(rs.getString("retryable_error_codes"))
                        )
                ))
                .optional()
                .orElseThrow(() -> new ExecutionConflictException(
                        "No running attempt exists for task " + taskId
                ));
    }

    private void updateWorkflow(WorkflowRun current, WorkflowRun next) {
        int changed = jdbc.sql("""
                UPDATE workflow_execution
                   SET status = :status,
                       state_version = :nextVersion,
                       started_at = :startedAt,
                       finished_at = :finishedAt
                 WHERE id = :id
                   AND state_version = :currentVersion
                """)
                .param("status", next.status().name())
                .param("nextVersion", next.stateVersion())
                .param("startedAt", timestamp(next.startedAt()))
                .param("finishedAt", timestamp(next.finishedAt()))
                .param("id", next.id())
                .param("currentVersion", current.stateVersion())
                .update();
        if (changed != 1) throw concurrentWorkflow(current);
    }

    private void updateTask(TaskRun current, TaskRun next) {
        int changed = jdbc.sql("""
                UPDATE task_execution
                   SET status = :status,
                       state_version = :nextVersion,
                       started_at = :startedAt,
                       finished_at = :finishedAt,
                       next_attempt_at = :nextAttemptAt
                 WHERE id = :id
                   AND state_version = :currentVersion
                """)
                .param("status", next.status().name())
                .param("nextVersion", next.stateVersion())
                .param("startedAt", timestamp(next.startedAt()))
                .param("finishedAt", timestamp(next.finishedAt()))
                .param("nextAttemptAt", timestamp(next.nextAttemptAt()))
                .param("id", next.id())
                .param("currentVersion", current.stateVersion())
                .update();
        if (changed != 1) {
            throw new ExecutionConflictException("Task " + current.id() + " was modified concurrently");
        }
    }

    private WorkflowRun lockWorkflow(UUID executionId) {
        return jdbc.sql("""
                SELECT id, workflow_id, workflow_version_number, status, state_version,
                       created_at, started_at, finished_at
                  FROM workflow_execution
                 WHERE id = :id
                 FOR UPDATE
                """)
                .param("id", executionId)
                .query((rs, rowNum) -> mapWorkflowRun(rs))
                .optional()
                .orElseThrow(() -> new ExecutionNotFoundException(executionId));
    }

    private WorkflowRun lockWorkflowForTask(UUID taskRunId) {
        return jdbc.sql("""
                SELECT we.id, we.workflow_id, we.workflow_version_number, we.status,
                       we.state_version, we.created_at, we.started_at, we.finished_at
                  FROM workflow_execution we
                  JOIN task_execution te ON te.workflow_execution_id = we.id
                 WHERE te.id = :taskId
                 FOR UPDATE OF we
                """)
                .param("taskId", taskRunId)
                .query((rs, rowNum) -> mapWorkflowRun(rs))
                .optional()
                .orElseThrow(() -> new ExecutionNotFoundException(taskRunId));
    }

    private TaskRun lockTask(UUID taskRunId) {
        return jdbc.sql("""
                SELECT id, workflow_execution_id, task_key, status, state_version,
                       created_at, started_at, finished_at, next_attempt_at
                  FROM task_execution
                 WHERE id = :id
                 FOR UPDATE
                """)
                .param("id", taskRunId)
                .query((rs, rowNum) -> mapTaskRun(rs))
                .single();
    }

    private Optional<WorkflowRun> findWorkflow(UUID executionId) {
        return jdbc.sql("""
                SELECT id, workflow_id, workflow_version_number, status, state_version,
                       created_at, started_at, finished_at
                  FROM workflow_execution
                 WHERE id = :id
                """)
                .param("id", executionId)
                .query((rs, rowNum) -> mapWorkflowRun(rs))
                .optional();
    }

    private StoredExecution storedExecution(UUID executionId) {
        return jdbc.sql("""
                SELECT id, workflow_version_id
                  FROM workflow_execution
                 WHERE id = :id
                """)
                .param("id", executionId)
                .query((rs, rowNum) -> new StoredExecution(
                        rs.getObject("id", UUID.class),
                        rs.getObject("workflow_version_id", UUID.class)
                ))
                .single();
    }

    private Optional<UUID> findByIdempotencyKey(UUID workflowId, String idempotencyKey) {
        return jdbc.sql("""
                SELECT id
                  FROM workflow_execution
                 WHERE workflow_id = :workflowId
                   AND idempotency_key = :idempotencyKey
                """)
                .param("workflowId", workflowId)
                .param("idempotencyKey", idempotencyKey)
                .query(UUID.class)
                .optional();
    }

    private WorkflowExecution get(UUID executionId) {
        return findWorkflow(executionId)
                .map(this::snapshot)
                .orElseThrow(() -> new ExecutionNotFoundException(executionId));
    }

    private WorkflowExecution snapshot(WorkflowRun workflow) {
        return new WorkflowExecution(
                workflow,
                loadTasks(workflow.id()),
                loadAttempts(workflow.id()),
                loadEvents(workflow.id())
        );
    }

    private List<TaskRun> loadTasks(UUID executionId) {
        return jdbc.sql("""
                SELECT te.id, te.workflow_execution_id, te.task_key, te.status, te.state_version,
                       te.created_at, te.started_at, te.finished_at, te.next_attempt_at
                  FROM task_execution te
                  JOIN workflow_execution we ON we.id = te.workflow_execution_id
                  JOIN workflow_task wt
                    ON wt.workflow_version_id = we.workflow_version_id
                   AND wt.task_key = te.task_key
                 WHERE te.workflow_execution_id = :executionId
                 ORDER BY wt.position
                """)
                .param("executionId", executionId)
                .query((rs, rowNum) -> mapTaskRun(rs))
                .list();
    }

    private List<TaskAttempt> loadAttempts(UUID executionId) {
        return jdbc.sql("""
                SELECT ta.id, ta.task_execution_id, ta.attempt_number, ta.status,
                       ta.started_at, ta.finished_at, ta.error_code, ta.error_message
                  FROM task_attempt ta
                  JOIN task_execution te ON te.id = ta.task_execution_id
                 WHERE te.workflow_execution_id = :executionId
                 ORDER BY ta.started_at, ta.task_execution_id, ta.attempt_number
                """)
                .param("executionId", executionId)
                .query((rs, rowNum) -> new TaskAttempt(
                        rs.getObject("id", UUID.class),
                        rs.getObject("task_execution_id", UUID.class),
                        rs.getInt("attempt_number"),
                        TaskAttemptStatus.valueOf(rs.getString("status")),
                        instant(rs.getObject("started_at")),
                        instant(rs.getObject("finished_at")),
                        rs.getString("error_code"),
                        rs.getString("error_message")
                ))
                .list();
    }

    private List<ExecutionEvent> loadEvents(UUID executionId) {
        return jdbc.sql("""
                SELECT id, workflow_execution_id, task_execution_id, event_type,
                       from_status, to_status, occurred_at
                  FROM execution_event
                 WHERE workflow_execution_id = :executionId
                 ORDER BY sequence_number
                """)
                .param("executionId", executionId)
                .query((rs, rowNum) -> new ExecutionEvent(
                        rs.getObject("id", UUID.class),
                        rs.getObject("workflow_execution_id", UUID.class),
                        rs.getObject("task_execution_id", UUID.class),
                        ExecutionEventType.valueOf(rs.getString("event_type")),
                        rs.getString("from_status"),
                        rs.getString("to_status"),
                        instant(rs.getObject("occurred_at"))
                ))
                .list();
    }

    private Map<String, TaskRunStatus> loadTaskStatuses(UUID executionId) {
        Map<String, TaskRunStatus> statuses = new LinkedHashMap<>();
        jdbc.sql("""
                SELECT task_key, status
                  FROM task_execution
                 WHERE workflow_execution_id = :executionId
                 ORDER BY task_key
                """)
                .param("executionId", executionId)
                .query((rs, rowNum) -> Map.entry(
                        rs.getString("task_key"),
                        TaskRunStatus.valueOf(rs.getString("status"))
                ))
                .list()
                .forEach(entry -> statuses.put(entry.getKey(), entry.getValue()));
        return statuses;
    }

    private List<TaskDependency> loadDependencies(UUID versionId) {
        return jdbc.sql("""
                SELECT task_key, depends_on_task_key
                  FROM workflow_dependency
                 WHERE workflow_version_id = :versionId
                 ORDER BY task_key, depends_on_task_key
                """)
                .param("versionId", versionId)
                .query((rs, rowNum) -> new TaskDependency(
                        rs.getString("task_key"),
                        rs.getString("depends_on_task_key")
                ))
                .list();
    }

    private boolean allTasksSucceeded(UUID executionId) {
        Map<String, TaskRunStatus> statuses = loadTaskStatuses(executionId);
        return !statuses.isEmpty()
                && statuses.values().stream()
                .allMatch(status -> status == TaskRunStatus.SUCCEEDED);
    }

    private boolean allTasksTerminal(UUID executionId) {
        Map<String, TaskRunStatus> statuses = loadTaskStatuses(executionId);
        return !statuses.isEmpty() && statuses.values().stream().allMatch(TaskRunStatus::isTerminal);
    }

    private long countTasksWithStatus(UUID executionId, TaskRunStatus status) {
        return jdbc.sql("""
                SELECT COUNT(*)
                  FROM task_execution
                 WHERE workflow_execution_id = :executionId
                   AND status = :status
                """)
                .param("executionId", executionId)
                .param("status", status.name())
                .query(Long.class)
                .single();
    }

    private void insertEvent(
            UUID executionId,
            UUID taskId,
            ExecutionEventType type,
            String fromStatus,
            String toStatus,
            Instant occurredAt
    ) {
        UUID eventId = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO execution_event(
                    id, workflow_execution_id, task_execution_id, event_type,
                    from_status, to_status, occurred_at
                ) VALUES (
                    :id, :executionId, :taskId, :eventType,
                    :fromStatus, :toStatus, :occurredAt
                )
                """)
                .param("id", eventId)
                .param("executionId", executionId)
                .param("taskId", taskId)
                .param("eventType", type.name())
                .param("fromStatus", fromStatus)
                .param("toStatus", toStatus)
                .param("occurredAt", timestamp(occurredAt))
                .update();
        MessageEnvelope<ExecutionEventV1> envelope = new MessageEnvelope<>(
                eventId,
                ExecutionEventV1.EVENT_TYPE,
                ExecutionEventV1.SCHEMA_VERSION,
                occurredAt,
                executionId,
                new ExecutionEventV1(executionId, taskId, type.name(), fromStatus, toStatus)
        );
        insertOutbox(
                eventId,
                executionId,
                taskId,
                eventId,
                "EXECUTION_EVENT",
                FlowForgeTopics.EXECUTION_EVENTS_V1,
                executionId.toString(),
                envelope.eventType(),
                envelope.schemaVersion(),
                toJson(envelope),
                occurredAt
        );
    }

    private void insertTaskCommand(TaskWorkItem workItem, Instant occurredAt) {
        UUID eventId = UUID.randomUUID();
        MessageEnvelope<TaskCommandV1> envelope = new MessageEnvelope<>(
                eventId,
                TaskCommandV1.EVENT_TYPE,
                TaskCommandV1.SCHEMA_VERSION,
                occurredAt,
                workItem.workflowRunId(),
                new TaskCommandV1(
                        workItem.workflowRunId(),
                        workItem.taskRunId(),
                        workItem.taskKey(),
                        workItem.taskType(),
                        workItem.configuration(),
                        workItem.stateVersion(),
                        workItem.attemptNumber(),
                        workItem.fencingToken(),
                        workItem.attemptTimeoutMs()
                )
        );
        insertOutbox(
                eventId,
                workItem.workflowRunId(),
                workItem.taskRunId(),
                null,
                "TASK_COMMAND",
                FlowForgeTopics.TASK_COMMANDS_V1,
                workItem.taskRunId().toString(),
                envelope.eventType(),
                envelope.schemaVersion(),
                toJson(envelope),
                occurredAt
        );
    }

    private void insertOutbox(
            UUID id,
            UUID executionId,
            UUID taskId,
            UUID sourceEventId,
            String messageKind,
            String topic,
            String recordKey,
            String eventType,
            int schemaVersion,
            String payload,
            Instant createdAt
    ) {
        jdbc.sql("""
                INSERT INTO control_plane_outbox(
                    id, workflow_execution_id, task_execution_id, source_event_id,
                    message_kind, topic, record_key, event_type, schema_version,
                    payload, status, available_at, created_at
                ) VALUES (
                    :id, :executionId, :taskId, :sourceEventId,
                    :messageKind, :topic, :recordKey, :eventType, :schemaVersion,
                    CAST(:payload AS jsonb), 'PENDING', :availableAt, :createdAt
                )
                """)
                .param("id", id)
                .param("executionId", executionId)
                .param("taskId", taskId)
                .param("sourceEventId", sourceEventId)
                .param("messageKind", messageKind)
                .param("topic", topic)
                .param("recordKey", recordKey)
                .param("eventType", eventType)
                .param("schemaVersion", schemaVersion)
                .param("payload", payload)
                .param("availableAt", timestamp(createdAt))
                .param("createdAt", timestamp(createdAt))
                .update();
    }

    private static WorkflowRun mapWorkflowRun(ResultSet rs) throws SQLException {
        return new WorkflowRun(
                rs.getObject("id", UUID.class),
                rs.getObject("workflow_id", UUID.class),
                rs.getInt("workflow_version_number"),
                WorkflowRunStatus.valueOf(rs.getString("status")),
                rs.getLong("state_version"),
                instant(rs.getObject("created_at")),
                instant(rs.getObject("started_at")),
                instant(rs.getObject("finished_at"))
        );
    }

    private static TaskRun mapTaskRun(ResultSet rs) throws SQLException {
        return new TaskRun(
                rs.getObject("id", UUID.class),
                rs.getObject("workflow_execution_id", UUID.class),
                rs.getString("task_key"),
                TaskRunStatus.valueOf(rs.getString("status")),
                rs.getLong("state_version"),
                instant(rs.getObject("created_at")),
                instant(rs.getObject("started_at")),
                instant(rs.getObject("finished_at")),
                instant(rs.getObject("next_attempt_at"))
        );
    }

    private Map<String, Object> fromJson(String value) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> configuration = objectMapper.readValue(value, Map.class);
            return configuration;
        } catch (JacksonException exception) {
            throw new IllegalStateException("Stored task configuration is invalid", exception);
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> stringSetFromJson(String value) {
        try {
            return Set.copyOf((Set<String>) objectMapper.readValue(value, Set.class));
        } catch (JacksonException exception) {
            throw new IllegalStateException("Stored retryable error codes are invalid", exception);
        }
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JacksonException exception) {
            throw new IllegalStateException("Could not serialize outbox message", exception);
        }
    }

    private static ExecutionEventType taskEvent(TaskRunStatus status) {
        return switch (status) {
            case SUCCEEDED -> ExecutionEventType.TASK_SUCCEEDED;
            case FAILED -> ExecutionEventType.TASK_FAILED;
            case TIMED_OUT -> ExecutionEventType.TASK_TIMED_OUT;
            case CANCELLED -> ExecutionEventType.TASK_CANCELLED;
            default -> throw new IllegalArgumentException("Status is not a task outcome: " + status);
        };
    }

    private static ExecutionConflictException concurrentWorkflow(WorkflowRun workflow) {
        return new ExecutionConflictException("Workflow execution " + workflow.id() + " was modified concurrently");
    }

    private static String truncate(String value, int maximumLength) {
        if (value == null || value.length() <= maximumLength) return value;
        return value.substring(0, maximumLength);
    }

    private static Instant instant(Object value) {
        if (value == null) return null;
        if (value instanceof OffsetDateTime timestamp) return timestamp.toInstant();
        if (value instanceof Timestamp timestamp) return timestamp.toInstant();
        if (value instanceof Instant instant) return instant;
        throw new IllegalStateException("Unsupported timestamp value: " + value.getClass());
    }

    private static Timestamp timestamp(Instant value) {
        return value == null ? null : Timestamp.from(value);
    }

    private record PublishedVersion(UUID id, int versionNumber, Integer maxConcurrentExecutions) {
    }

    private record StoredExecution(UUID id, UUID workflowVersionId) {
    }

    private record TaskTemplate(String taskKey, String taskType, Map<String, Object> configuration) {
    }

    private record ClaimCandidate(
            TaskRun task,
            String taskType,
            Map<String, Object> configuration,
            int attemptNumber,
            Long attemptTimeoutMs,
            UUID workflowVersionId,
            Integer maxConcurrency
    ) {
    }

    private record RetryContext(int attemptNumber, UUID fencingToken, TaskReliabilityPolicy policy) {
    }

    private record LeaseCandidate(UUID taskId, UUID fencingToken) {
    }

    private record CompletedAttempt(UUID concurrencyPermitToken) {
    }
}
