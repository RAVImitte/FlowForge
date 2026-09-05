package io.flowforge.controlplane.adapter.out.persistence;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import io.flowforge.application.workflow.PageResult;
import io.flowforge.application.workflow.WorkflowConflictException;
import io.flowforge.application.workflow.WorkflowNotFoundException;
import io.flowforge.application.workflow.WorkflowRepository;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.TaskDependency;
import io.flowforge.domain.workflow.TaskReliabilityPolicy;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import io.flowforge.domain.workflow.WorkflowLifecycleStatus;
import io.flowforge.domain.workflow.WorkflowVersionStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Repository
public class JdbcWorkflowRepository implements WorkflowRepository {
    private static final String SELECT_CURRENT = """
            SELECT w.id, w.lock_version, w.lifecycle_status, w.created_at, w.updated_at,
                   v.id AS workflow_version_id, v.version_number, v.version_status,
                   v.name, v.description, v.max_concurrent_executions, v.published_at
              FROM workflow_definition w
              JOIN LATERAL (
                    SELECT candidate.*
                      FROM workflow_version candidate
                     WHERE candidate.workflow_id = w.id
                     ORDER BY CASE WHEN candidate.version_status = 'DRAFT' THEN 0 ELSE 1 END,
                              candidate.version_number DESC
                     LIMIT 1
              ) v ON TRUE
             WHERE w.id = :id
               AND w.lifecycle_status = 'ACTIVE'
            """;

    private final JdbcClient jdbc;
    private final ObjectMapper objectMapper;

    public JdbcWorkflowRepository(JdbcClient jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    @Override
    @Transactional
    public WorkflowDefinition create(WorkflowDraft draft) {
        UUID workflowId = UUID.randomUUID();
        UUID versionId = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO workflow_definition(id, lifecycle_status)
                VALUES (:id, 'ACTIVE')
                """).param("id", workflowId).update();
        jdbc.sql("""
                INSERT INTO workflow_version(
                    id, workflow_id, version_number, version_status, name, description,
                    max_concurrent_executions
                ) VALUES (
                    :id, :workflowId, 1, 'DRAFT', :name, :description,
                    :maxConcurrentExecutions
                )
                """)
                .param("id", versionId)
                .param("workflowId", workflowId)
                .param("name", draft.name())
                .param("description", draft.description())
                .param("maxConcurrentExecutions", draft.maxConcurrentExecutions())
                .update();
        replaceGraph(versionId, draft);
        return findById(workflowId).orElseThrow();
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<WorkflowDefinition> findById(UUID id) {
        return jdbc.sql(SELECT_CURRENT)
                .param("id", id)
                .query((rs, rowNum) -> {
                    UUID versionId = rs.getObject("workflow_version_id", UUID.class);
                    return new WorkflowDefinition(
                            rs.getObject("id", UUID.class),
                            rs.getLong("lock_version"),
                            WorkflowLifecycleStatus.valueOf(rs.getString("lifecycle_status")),
                            rs.getInt("version_number"),
                            WorkflowVersionStatus.valueOf(rs.getString("version_status")),
                            rs.getString("name"),
                            rs.getString("description"),
                            (Integer) rs.getObject("max_concurrent_executions"),
                            loadTasks(versionId),
                            loadDependencies(versionId),
                            instant(rs.getObject("created_at")),
                            instant(rs.getObject("updated_at")),
                            instant(rs.getObject("published_at"))
                    );
                })
                .optional();
    }

    @Override
    @Transactional(readOnly = true)
    public PageResult<WorkflowDefinition> findAll(int page, int size) {
        long total = jdbc.sql("""
                SELECT COUNT(*) FROM workflow_definition WHERE lifecycle_status = 'ACTIVE'
                """).query(Long.class).single();
        List<UUID> ids = jdbc.sql("""
                SELECT id
                  FROM workflow_definition
                 WHERE lifecycle_status = 'ACTIVE'
                 ORDER BY created_at DESC, id
                 LIMIT :limit OFFSET :offset
                """)
                .param("limit", size)
                .param("offset", page * size)
                .query(UUID.class)
                .list();
        List<WorkflowDefinition> workflows = ids.stream()
                .map(id -> findById(id).orElseThrow())
                .toList();
        return new PageResult<>(workflows, page, size, total);
    }

    @Override
    @Transactional(readOnly = true)
    public boolean hasPublishedVersion(UUID id) {
        return jdbc.sql("""
                SELECT EXISTS (
                    SELECT 1
                      FROM workflow_definition w
                      JOIN workflow_version v ON v.workflow_id = w.id
                     WHERE w.id = :id
                       AND w.lifecycle_status = 'ACTIVE'
                       AND v.version_status = 'PUBLISHED'
                )
                """).param("id", id).query(Boolean.class).single();
    }

    @Override
    @Transactional
    public WorkflowDefinition update(UUID id, long expectedLockVersion, WorkflowDraft draft) {
        lockAndCheck(id, expectedLockVersion);
        Optional<UUID> existingDraft = jdbc.sql("""
                SELECT id FROM workflow_version
                 WHERE workflow_id = :workflowId AND version_status = 'DRAFT'
                """).param("workflowId", id).query(UUID.class).optional();

        UUID versionId;
        if (existingDraft.isPresent()) {
            versionId = existingDraft.get();
            jdbc.sql("""
                    UPDATE workflow_version
                       SET name = :name,
                           description = :description,
                           max_concurrent_executions = :maxConcurrentExecutions,
                           updated_at = CURRENT_TIMESTAMP
                     WHERE id = :id
                    """)
                    .param("name", draft.name())
                    .param("description", draft.description())
                    .param("maxConcurrentExecutions", draft.maxConcurrentExecutions())
                    .param("id", versionId)
                    .update();
        } else {
            int nextVersion = jdbc.sql("""
                    SELECT COALESCE(MAX(version_number), 0) + 1
                      FROM workflow_version WHERE workflow_id = :workflowId
                    """).param("workflowId", id).query(Integer.class).single();
            versionId = UUID.randomUUID();
            jdbc.sql("""
                    INSERT INTO workflow_version(
                        id, workflow_id, version_number, version_status, name, description,
                        max_concurrent_executions
                    ) VALUES (
                        :id, :workflowId, :versionNumber, 'DRAFT', :name, :description,
                        :maxConcurrentExecutions
                    )
                    """)
                    .param("id", versionId)
                    .param("workflowId", id)
                    .param("versionNumber", nextVersion)
                    .param("name", draft.name())
                    .param("description", draft.description())
                    .param("maxConcurrentExecutions", draft.maxConcurrentExecutions())
                    .update();
        }
        replaceGraph(versionId, draft);
        incrementVersion(id);
        return findById(id).orElseThrow();
    }

    @Override
    @Transactional
    public WorkflowDefinition publish(UUID id, long expectedLockVersion) {
        lockAndCheck(id, expectedLockVersion);
        int changed = jdbc.sql("""
                UPDATE workflow_version
                   SET version_status = 'PUBLISHED',
                       published_at = CURRENT_TIMESTAMP,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE workflow_id = :workflowId
                   AND version_status = 'DRAFT'
                """).param("workflowId", id).update();
        if (changed == 0) {
            throw new WorkflowConflictException("Workflow has no draft version to publish");
        }
        incrementVersion(id);
        return findById(id).orElseThrow();
    }

    @Override
    @Transactional
    public void archive(UUID id, long expectedLockVersion) {
        lockAndCheck(id, expectedLockVersion);
        jdbc.sql("""
                UPDATE workflow_definition
                   SET lifecycle_status = 'ARCHIVED',
                       lock_version = lock_version + 1,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = :id
                """).param("id", id).update();
    }

    private void lockAndCheck(UUID id, long expectedLockVersion) {
        Optional<Map<String, Object>> row = jdbc.sql("""
                SELECT lock_version, lifecycle_status
                  FROM workflow_definition
                 WHERE id = :id
                 FOR UPDATE
                """).param("id", id).query((rs, rowNum) -> Map.<String, Object>of("lock_version", rs.getLong("lock_version"), "lifecycle_status", rs.getString("lifecycle_status"))).optional();
        if (row.isEmpty() || !"ACTIVE".equals(row.get().get("lifecycle_status"))) {
            throw new WorkflowNotFoundException(id);
        }
        long actual = ((Number) row.get().get("lock_version")).longValue();
        if (actual != expectedLockVersion) {
            throw new WorkflowConflictException(
                    "Workflow was modified concurrently; expected version "
                            + expectedLockVersion + " but found " + actual);
        }
    }

    private void incrementVersion(UUID id) {
        jdbc.sql("""
                UPDATE workflow_definition
                   SET lock_version = lock_version + 1, updated_at = CURRENT_TIMESTAMP
                 WHERE id = :id
                """).param("id", id).update();
    }

    private void replaceGraph(UUID versionId, WorkflowDraft draft) {
        jdbc.sql("DELETE FROM workflow_dependency WHERE workflow_version_id = :versionId")
                .param("versionId", versionId).update();
        jdbc.sql("DELETE FROM workflow_task WHERE workflow_version_id = :versionId")
                .param("versionId", versionId).update();

        for (int position = 0; position < draft.tasks().size(); position++) {
            TaskDefinition task = draft.tasks().get(position);
            jdbc.sql("""
                    INSERT INTO workflow_task(
                        id, workflow_version_id, task_key, task_name, task_type, configuration, position,
                        max_attempts, initial_backoff_ms, backoff_multiplier, max_backoff_ms,
                        jitter_factor, attempt_timeout_ms, retryable_error_codes, max_concurrency
                    ) VALUES (
                        :id, :versionId, :taskKey, :taskName, :taskType,
                        CAST(:configuration AS jsonb), :position,
                        :maxAttempts, :initialBackoffMs, :backoffMultiplier, :maxBackoffMs,
                        :jitterFactor, :attemptTimeoutMs, CAST(:retryableErrorCodes AS jsonb),
                        :maxConcurrency
                    )
                    """)
                    .param("id", UUID.randomUUID())
                    .param("versionId", versionId)
                    .param("taskKey", task.key())
                    .param("taskName", task.name())
                    .param("taskType", task.type())
                    .param("configuration", toJson(task.configuration()))
                    .param("position", position)
                    .param("maxAttempts", task.reliabilityPolicy().maxAttempts())
                    .param("initialBackoffMs", task.reliabilityPolicy().initialBackoff().toMillis())
                    .param("backoffMultiplier", task.reliabilityPolicy().backoffMultiplier())
                    .param("maxBackoffMs", task.reliabilityPolicy().maxBackoff().toMillis())
                    .param("jitterFactor", task.reliabilityPolicy().jitterFactor())
                    .param("attemptTimeoutMs", task.reliabilityPolicy().attemptTimeout() == null
                            ? null
                            : task.reliabilityPolicy().attemptTimeout().toMillis())
                    .param("retryableErrorCodes", toJson(task.reliabilityPolicy().retryableErrorCodes()))
                    .param("maxConcurrency", task.maxConcurrency())
                    .update();
        }
        for (TaskDependency dependency : draft.dependencies()) {
            jdbc.sql("""
                    INSERT INTO workflow_dependency(
                        workflow_version_id, task_key, depends_on_task_key
                    ) VALUES (:versionId, :taskKey, :dependsOnTaskKey)
                    """)
                    .param("versionId", versionId)
                    .param("taskKey", dependency.taskKey())
                    .param("dependsOnTaskKey", dependency.dependsOnTaskKey())
                    .update();
        }
    }

    private List<TaskDefinition> loadTasks(UUID versionId) {
        return jdbc.sql("""
                SELECT task_key, task_name, task_type, configuration,
                       max_attempts, initial_backoff_ms, backoff_multiplier, max_backoff_ms,
                       jitter_factor, attempt_timeout_ms, retryable_error_codes, max_concurrency
                  FROM workflow_task
                 WHERE workflow_version_id = :versionId
                 ORDER BY position
                """)
                .param("versionId", versionId)
                .query((rs, rowNum) -> new TaskDefinition(
                        rs.getString("task_key"),
                        rs.getString("task_name"),
                        rs.getString("task_type"),
                        fromJson(rs.getString("configuration")),
                        new TaskReliabilityPolicy(
                                rs.getInt("max_attempts"),
                                Duration.ofMillis(rs.getLong("initial_backoff_ms")),
                                rs.getDouble("backoff_multiplier"),
                                Duration.ofMillis(rs.getLong("max_backoff_ms")),
                                rs.getDouble("jitter_factor"),
                                nullableDuration(rs.getObject("attempt_timeout_ms")),
                                stringSetFromJson(rs.getString("retryable_error_codes"))
                        ),
                        (Integer) rs.getObject("max_concurrency")
                ))
                .list();
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

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JacksonException exception) {
            throw new IllegalArgumentException("Task configuration is not JSON serializable", exception);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> fromJson(String value) {
        try {
            return objectMapper.readValue(value, Map.class);
        } catch (JacksonException exception) {
            throw new IllegalStateException("Stored task configuration is invalid", exception);
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> stringSetFromJson(String value) {
        try {
            return Set.copyOf(objectMapper.readValue(value, Set.class));
        } catch (JacksonException exception) {
            throw new IllegalStateException("Stored retryable error codes are invalid", exception);
        }
    }

    private static Duration nullableDuration(Object value) {
        return value == null ? null : Duration.ofMillis(((Number) value).longValue());
    }

    private static Instant instant(Object value) {
        if (value == null) return null;
        if (value instanceof OffsetDateTime timestamp) return timestamp.toInstant();
        if (value instanceof Timestamp timestamp) return timestamp.toInstant();
        throw new IllegalStateException("Unsupported timestamp value: " + value.getClass());
    }
}
