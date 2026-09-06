package io.flowforge.controlplane;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
class TenantMigrationIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @Test
    void upgradesExistingOwnershipRootsWithoutLosingOrOrphaningData() {
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .target("12")
                .load()
                .migrate();

        JdbcClient jdbc = JdbcClient.create(new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()
        ));
        UUID workflowId = UUID.randomUUID();
        UUID versionId = UUID.randomUUID();
        UUID executionId = UUID.randomUUID();
        UUID scheduleId = UUID.randomUUID();
        UUID triggerId = UUID.randomUUID();
        UUID permitToken = UUID.randomUUID();
        String resourceKey = "migration-resource/" + UUID.randomUUID();
        String bucketKey = "migration-bucket/" + UUID.randomUUID();

        jdbc.sql("""
                INSERT INTO workflow_definition (id, lifecycle_status)
                VALUES (:id, 'ACTIVE')
                """).param("id", workflowId).update();
        jdbc.sql("""
                INSERT INTO workflow_version (
                    id, workflow_id, version_number, version_status, name
                ) VALUES (:id, :workflowId, 1, 'DRAFT', 'Existing workflow')
                """)
                .param("id", versionId)
                .param("workflowId", workflowId)
                .update();
        jdbc.sql("""
                INSERT INTO workflow_execution (
                    id, workflow_id, workflow_version_id, workflow_version_number,
                    idempotency_key, status, created_at
                ) VALUES (
                    :id, :workflowId, :versionId, 1, 'existing-execution', 'PENDING', CURRENT_TIMESTAMP
                )
                """)
                .param("id", executionId)
                .param("workflowId", workflowId)
                .param("versionId", versionId)
                .update();
        jdbc.sql("""
                INSERT INTO workflow_schedule (
                    id, workflow_id, schedule_type, one_time_at, misfire_policy,
                    status, next_fire_at, created_at, updated_at
                ) VALUES (
                    :id, :workflowId, 'ONE_TIME', CURRENT_TIMESTAMP + INTERVAL '1 hour',
                    'FIRE_ONCE', 'ACTIVE', CURRENT_TIMESTAMP + INTERVAL '1 hour',
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                """)
                .param("id", scheduleId)
                .param("workflowId", workflowId)
                .update();
        jdbc.sql("""
                INSERT INTO workflow_schedule_trigger (
                    id, schedule_id, workflow_id, scheduled_fire_at, idempotency_key,
                    status, attempt_count, available_at, created_at
                ) VALUES (
                    :id, :scheduleId, :workflowId, CURRENT_TIMESTAMP,
                    :idempotencyKey, 'PENDING', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                """)
                .param("id", triggerId)
                .param("scheduleId", scheduleId)
                .param("workflowId", workflowId)
                .param("idempotencyKey", "migration-trigger/" + triggerId)
                .update();
        jdbc.sql("""
                INSERT INTO coordination_resource(resource_key, created_at, updated_at)
                VALUES (:resourceKey, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """)
                .param("resourceKey", resourceKey)
                .update();
        jdbc.sql("""
                INSERT INTO coordination_permit(
                    token, resource_key, holder_id, status, expires_at,
                    created_at, renewed_at
                ) VALUES (
                    :token, :resourceKey, 'migration-holder', 'ACTIVE',
                    CURRENT_TIMESTAMP + INTERVAL '1 hour', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                """)
                .param("token", permitToken)
                .param("resourceKey", resourceKey)
                .update();
        jdbc.sql("""
                UPDATE workflow_execution
                   SET concurrency_permit_token = :token
                 WHERE id = :executionId
                """)
                .param("token", permitToken)
                .param("executionId", executionId)
                .update();
        jdbc.sql("""
                INSERT INTO admission_rate_bucket(
                    bucket_key, capacity, refill_tokens, refill_period_ms,
                    available_tokens, last_refill_at, updated_at
                ) VALUES (
                    :bucketKey, 10, 1, 1000, 9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                """)
                .param("bucketKey", bucketKey)
                .update();

        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();

        assertThat(tenant(jdbc, "workflow_definition", workflowId)).isEqualTo("local");
        assertThat(tenant(jdbc, "workflow_execution", executionId)).isEqualTo("local");
        assertThat(tenant(jdbc, "workflow_schedule", scheduleId)).isEqualTo("local");
        assertThat(tenant(jdbc, "workflow_schedule_trigger", triggerId)).isEqualTo("local");
        assertThat(jdbc.sql("SELECT tenant_id FROM coordination_permit WHERE token = :token")
                .param("token", permitToken)
                .query(String.class).single()).isEqualTo("local");
        assertThat(jdbc.sql("SELECT tenant_id FROM coordination_resource WHERE resource_key = :key")
                .param("key", resourceKey)
                .query(String.class).single()).isEqualTo("local");
        assertThat(jdbc.sql("SELECT tenant_id FROM admission_rate_bucket WHERE bucket_key = :key")
                .param("key", bucketKey)
                .query(String.class).single()).isEqualTo("local");
        assertThat(columnDefault(jdbc, "workflow_definition", "tenant_id")).isNull();
        assertThat(columnDefault(jdbc, "workflow_execution", "tenant_id")).isNull();
        assertThat(columnDefault(jdbc, "workflow_schedule", "tenant_id")).isNull();
        assertThat(jdbc.sql("SELECT COUNT(*) FROM tenant_registry WHERE tenant_id = 'local'")
                .query(Long.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT to_regclass('public.tenant_quota') IS NOT NULL")
                .query(Boolean.class).single()).isTrue();
        assertThat(jdbc.sql("SELECT to_regclass('public.security_audit_event') IS NOT NULL")
                .query(Boolean.class).single()).isTrue();
        assertThat(jdbc.sql("""
                SELECT COUNT(*) FROM information_schema.columns
                 WHERE table_schema = 'public'
                   AND table_name = 'workflow_task'
                   AND column_name = 'secret_references'
                """).query(Long.class).single()).isEqualTo(1);
    }

    private static String tenant(JdbcClient jdbc, String table, UUID id) {
        return jdbc.sql("SELECT tenant_id FROM " + table + " WHERE id = :id")
                .param("id", id)
                .query(String.class)
                .single();
    }

    private static String columnDefault(JdbcClient jdbc, String table, String column) {
        return jdbc.sql("""
                SELECT column_default
                  FROM information_schema.columns
                 WHERE table_schema = 'public'
                   AND table_name = :table
                   AND column_name = :column
                """)
                .param("table", table)
                .param("column", column)
                .query(String.class)
                .optional()
                .orElse(null);
    }
}
