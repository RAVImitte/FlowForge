package io.flowforge.controlplane;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers(disabledWithoutDocker = true)
class PostgresBackupRestoreIntegrationTest {
    private static final String DUMP_PATH = "/tmp/flowforge-recovery.dump";

    @org.testcontainers.junit.jupiter.Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @Test
    void restoresAnAuthoritativeSnapshotWithSchemaTenantAndAuditInvariants() throws Exception {
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();

        JdbcClient source = jdbc(POSTGRES.getDatabaseName());
        RecoveryFixture fixture = seedRecoveryFixture(source);
        Map<String, Long> recoveryPoint = tableCounts(source);

        exec("create a custom-format backup", "sh", "-ceu", """
                PGPASSWORD="$POSTGRES_PASSWORD" pg_dump \
                  --host=127.0.0.1 \
                  --username="$POSTGRES_USER" \
                  --dbname="$POSTGRES_DB" \
                  --format=custom \
                  --compress=6 \
                  --no-owner \
                  --no-privileges \
                  --file=%s
                """.formatted(DUMP_PATH));
        Container.ExecResult listing = exec(
                "validate the backup catalog", "pg_restore", "--list", DUMP_PATH
        );
        assertThat(listing.getStdout())
                .contains("TABLE DATA public workflow_definition")
                .contains("TABLE DATA public workflow_execution")
                .contains("TABLE DATA public security_audit_event");

        source.sql("""
                INSERT INTO tenant_registry(tenant_id, display_name, status)
                VALUES ('recovery-after-snapshot', 'Created after recovery point', 'ACTIVE')
                """).update();
        assertThat(tableCounts(source)).isNotEqualTo(recoveryPoint);

        String restoredDatabase = "flowforge_restore_" + UUID.randomUUID().toString().replace("-", "");
        boolean created = false;
        try {
            exec("create an isolated restore database", "sh", "-ceu", """
                    PGPASSWORD="$POSTGRES_PASSWORD" createdb \
                      --host=127.0.0.1 \
                      --username="$POSTGRES_USER" \
                      %s
                    """.formatted(restoredDatabase));
            created = true;
            exec("restore the snapshot transactionally", "sh", "-ceu", """
                    PGPASSWORD="$POSTGRES_PASSWORD" pg_restore \
                      --host=127.0.0.1 \
                      --username="$POSTGRES_USER" \
                      --dbname=%s \
                      --single-transaction \
                      --exit-on-error \
                      --no-owner \
                      --no-privileges \
                      %s
                    """.formatted(restoredDatabase, DUMP_PATH));

            JdbcClient restored = jdbc(restoredDatabase);
            Flyway.configure()
                    .dataSource(jdbcUrl(restoredDatabase), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .locations("classpath:db/migration")
                    .load()
                    .validate();

            assertThat(tableCounts(restored)).isEqualTo(recoveryPoint);
            assertThat(restored.sql("""
                    SELECT COUNT(*) FROM tenant_registry
                     WHERE tenant_id = 'recovery-after-snapshot'
                    """).query(Long.class).single()).isZero();
            assertThat(restored.sql("""
                    SELECT secret_references::text FROM workflow_task
                     WHERE workflow_version_id = :versionId AND task_key = 'RECOVER'
                    """)
                    .param("versionId", fixture.workflowVersionId())
                    .query(String.class)
                    .single()).contains("vault/flowforge/recovery");
            assertThat(restored.sql("""
                    SELECT COUNT(*) FROM coordination_permit
                     WHERE tenant_id = :tenantId AND status = 'ACTIVE'
                    """)
                    .param("tenantId", fixture.tenantId())
                    .query(Long.class)
                    .single()).isEqualTo(1);
            assertThat(restored.sql("""
                    SELECT version FROM flyway_schema_history
                     WHERE success ORDER BY installed_rank DESC LIMIT 1
                    """).query(String.class).single()).isEqualTo("19");

            assertThatThrownBy(() -> restored.sql("""
                    UPDATE security_audit_event SET actor = 'tampered' WHERE id = :id
                    """).param("id", fixture.auditId()).update())
                    .isInstanceOf(DataAccessException.class)
                    .hasMessageContaining("append-only");
            assertThatThrownBy(() -> restored.sql("""
                    UPDATE workflow_execution SET tenant_id = 'recovery-tenant-b' WHERE id = :id
                    """).param("id", fixture.executionId()).update())
                    .isInstanceOf(DataAccessException.class);
        } finally {
            if (created) {
                exec("drop the isolated restore database", "sh", "-ceu", """
                        PGPASSWORD="$POSTGRES_PASSWORD" dropdb \
                          --host=127.0.0.1 \
                          --username="$POSTGRES_USER" \
                          --force \
                          %s
                        """.formatted(restoredDatabase));
            }
        }
    }

    private static RecoveryFixture seedRecoveryFixture(JdbcClient jdbc) {
        String tenantId = "recovery-tenant-a";
        UUID workflowId = UUID.randomUUID();
        UUID workflowVersionId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        UUID executionId = UUID.randomUUID();
        UUID taskExecutionId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        UUID permitToken = UUID.randomUUID();
        UUID auditId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();

        jdbc.sql("""
                INSERT INTO tenant_registry(tenant_id, display_name, status)
                VALUES (:tenantId, 'Recovery tenant A', 'ACTIVE'),
                       ('recovery-tenant-b', 'Recovery tenant B', 'ACTIVE')
                """).param("tenantId", tenantId).update();
        jdbc.sql("""
                INSERT INTO workflow_definition(id, lifecycle_status, tenant_id)
                VALUES (:id, 'ACTIVE', :tenantId)
                """).param("id", workflowId).param("tenantId", tenantId).update();
        jdbc.sql("""
                INSERT INTO workflow_version(
                    id, workflow_id, version_number, version_status, name, published_at
                ) VALUES (:id, :workflowId, 1, 'PUBLISHED', 'Recoverable workflow', CURRENT_TIMESTAMP)
                """).param("id", workflowVersionId).param("workflowId", workflowId).update();
        jdbc.sql("""
                INSERT INTO workflow_task(
                    id, workflow_version_id, task_key, task_name, task_type,
                    configuration, position, secret_references
                ) VALUES (
                    :id, :versionId, 'RECOVER', 'Recover state', 'NOOP', '{}'::jsonb, 0,
                    '{"apiToken":{"provider":"vault","path":"vault/flowforge/recovery","key":"token"}}'::jsonb
                )
                """).param("id", taskId).param("versionId", workflowVersionId).update();
        jdbc.sql("""
                INSERT INTO workflow_execution(
                    id, workflow_id, workflow_version_id, workflow_version_number,
                    idempotency_key, status, created_at, tenant_id
                ) VALUES (
                    :id, :workflowId, :versionId, 1, 'recovery-snapshot', 'PENDING',
                    CURRENT_TIMESTAMP, :tenantId
                )
                """)
                .param("id", executionId)
                .param("workflowId", workflowId)
                .param("versionId", workflowVersionId)
                .param("tenantId", tenantId)
                .update();
        jdbc.sql("""
                INSERT INTO task_execution(
                    id, workflow_execution_id, task_key, status, created_at
                ) VALUES (:id, :executionId, 'RECOVER', 'READY', CURRENT_TIMESTAMP)
                """).param("id", taskExecutionId).param("executionId", executionId).update();
        jdbc.sql("""
                INSERT INTO execution_event(
                    id, workflow_execution_id, event_type, to_status, occurred_at, tenant_id
                ) VALUES (:id, :executionId, 'WORKFLOW_STARTED', 'PENDING', CURRENT_TIMESTAMP, :tenantId)
                """)
                .param("id", eventId)
                .param("executionId", executionId)
                .param("tenantId", tenantId)
                .update();
        jdbc.sql("""
                INSERT INTO tenant_quota(
                    tenant_id, quota_version, max_active_executions, max_running_tasks,
                    max_pending_schedule_fires, max_ready_tasks,
                    schedule_rate_capacity, schedule_rate_refill_tokens, schedule_rate_refill_period_ms,
                    dispatch_rate_capacity, dispatch_rate_refill_tokens, dispatch_rate_refill_period_ms,
                    created_at, updated_at
                ) VALUES (
                    :tenantId, 1, 100, 100, 100, 100,
                    100, 10, 1000, 100, 10, 1000, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                """).param("tenantId", tenantId).update();
        jdbc.sql("""
                INSERT INTO coordination_resource(tenant_id, resource_key, created_at, updated_at)
                VALUES (:tenantId, 'recovery/resource', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """).param("tenantId", tenantId).update();
        jdbc.sql("""
                INSERT INTO coordination_permit(
                    token, tenant_id, resource_key, holder_id, status,
                    expires_at, created_at, renewed_at
                ) VALUES (
                    :token, :tenantId, 'recovery/resource', 'recovery-holder', 'ACTIVE',
                    CURRENT_TIMESTAMP + INTERVAL '1 hour', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                """).param("token", permitToken).param("tenantId", tenantId).update();
        jdbc.sql("""
                INSERT INTO security_audit_event(
                    id, occurred_at, tenant_id, actor, action, target_type, target_id,
                    outcome, http_method, http_path, status_code, request_id
                ) VALUES (
                    :id, CURRENT_TIMESTAMP, :tenantId, 'recovery-operator', 'BACKUP_VERIFY',
                    'DATABASE', 'flowforge', 'SUCCEEDED', 'POST', '/operations/recovery', 200, :requestId
                )
                """)
                .param("id", auditId)
                .param("tenantId", tenantId)
                .param("requestId", requestId)
                .update();
        return new RecoveryFixture(tenantId, workflowVersionId, executionId, auditId);
    }

    private static Map<String, Long> tableCounts(JdbcClient jdbc) {
        Map<String, Long> counts = new TreeMap<>();
        jdbc.sql("""
                SELECT table_name FROM information_schema.tables
                 WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                 ORDER BY table_name
                """).query(String.class).list().forEach(table -> {
            if (!table.matches("[a-z0-9_]+")) throw new IllegalStateException("Unsafe table name " + table);
            counts.put(table, jdbc.sql("SELECT COUNT(*) FROM \"" + table + "\"")
                    .query(Long.class).single());
        });
        return counts;
    }

    private static JdbcClient jdbc(String database) {
        return JdbcClient.create(new DriverManagerDataSource(
                jdbcUrl(database), POSTGRES.getUsername(), POSTGRES.getPassword()
        ));
    }

    private static String jdbcUrl(String database) {
        return "jdbc:postgresql://%s:%d/%s".formatted(
                POSTGRES.getHost(), POSTGRES.getMappedPort(5432), database
        );
    }

    private static Container.ExecResult exec(String description, String... command) throws Exception {
        Container.ExecResult result = POSTGRES.execInContainer(command);
        assertThat(result.getExitCode())
                .as("%s failed: %s", description, result.getStderr())
                .isZero();
        return result;
    }

    private record RecoveryFixture(
            String tenantId,
            UUID workflowVersionId,
            UUID executionId,
            UUID auditId
    ) {
    }
}
