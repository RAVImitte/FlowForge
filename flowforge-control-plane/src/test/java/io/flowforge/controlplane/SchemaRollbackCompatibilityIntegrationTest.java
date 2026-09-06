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
class SchemaRollbackCompatibilityIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @Test
    void keepsThePreviousApplicationSqlContractUsableAfterTheV18AdditiveMigration() {
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .target("17")
                .load()
                .migrate();

        JdbcClient jdbc = JdbcClient.create(new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()
        ));

        UUID existingWorkflowId = UUID.randomUUID();
        UUID existingVersionId = UUID.randomUUID();
        UUID existingTaskId = UUID.randomUUID();
        insertUsingV17Contract(jdbc, existingWorkflowId, existingVersionId, existingTaskId, "existing");

        Flyway current = Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .target("18")
                .load();
        current.migrate();

        assertThat(current.info().current().getVersion().getVersion()).isEqualTo("18");
        assertThat(secretReferences(jdbc, existingTaskId)).isEqualTo("{}");

        UUID rollbackWorkflowId = UUID.randomUUID();
        UUID rollbackVersionId = UUID.randomUUID();
        UUID rollbackTaskId = UUID.randomUUID();
        insertUsingV17Contract(jdbc, rollbackWorkflowId, rollbackVersionId, rollbackTaskId, "rollback");

        int updated = jdbc.sql("""
                        UPDATE workflow_task
                           SET task_name = :name,
                               configuration = CAST(:configuration AS jsonb)
                         WHERE id = :id
                        """)
                .param("name", "Updated by V17 application")
                .param("configuration", "{\"mode\":\"legacy\"}")
                .param("id", rollbackTaskId)
                .update();

        assertThat(updated).isOne();
        assertThat(jdbc.sql("SELECT task_name FROM workflow_task WHERE id = :id")
                .param("id", rollbackTaskId)
                .query(String.class)
                .single()).isEqualTo("Updated by V17 application");
        assertThat(secretReferences(jdbc, rollbackTaskId)).isEqualTo("{}");
    }

    private static void insertUsingV17Contract(
            JdbcClient jdbc,
            UUID workflowId,
            UUID versionId,
            UUID taskId,
            String suffix
    ) {
        jdbc.sql("""
                        INSERT INTO workflow_definition (id, lifecycle_status, tenant_id)
                        VALUES (:id, 'ACTIVE', 'local')
                        """)
                .param("id", workflowId)
                .update();

        jdbc.sql("""
                        INSERT INTO workflow_version (
                            id, workflow_id, version_number, version_status, name
                        ) VALUES (:id, :workflowId, 1, 'DRAFT', :name)
                        """)
                .param("id", versionId)
                .param("workflowId", workflowId)
                .param("name", "V17 " + suffix + " workflow")
                .update();

        jdbc.sql("""
                        INSERT INTO workflow_task (
                            id, workflow_version_id, task_key, task_name,
                            task_type, configuration, position
                        ) VALUES (
                            :id, :versionId, :taskKey, :taskName,
                            'HTTP', CAST(:configuration AS jsonb), 0
                        )
                        """)
                .param("id", taskId)
                .param("versionId", versionId)
                .param("taskKey", "task-" + suffix)
                .param("taskName", "V17 " + suffix + " task")
                .param("configuration", "{\"source\":\"v17\"}")
                .update();
    }

    private static String secretReferences(JdbcClient jdbc, UUID taskId) {
        return jdbc.sql("SELECT secret_references::text FROM workflow_task WHERE id = :id")
                .param("id", taskId)
                .query(String.class)
                .single();
    }
}
