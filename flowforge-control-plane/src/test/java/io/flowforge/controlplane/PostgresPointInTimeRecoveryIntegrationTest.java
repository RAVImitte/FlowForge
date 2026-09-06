package io.flowforge.controlplane;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
class PostgresPointInTimeRecoveryIntegrationTest {
    private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse("postgres:17.6-alpine");
    private static final String RESTORE_POINT = "flowforge_before_loss";

    @Test
    void restoresAValidatedBaseBackupToANamedWalRecoveryPoint() throws Exception {
        Path evidence = Files.createTempDirectory("flowforge-pitr-");
        try {
            captureBaseBackupAndWal(evidence);
            restoreAndVerifyRecoveryPoint(evidence);
        } finally {
            try (var paths = Files.walk(evidence)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (Exception failure) {
                        throw new IllegalStateException("Could not remove PITR evidence " + path, failure);
                    }
                });
            }
        }
    }

    private static void captureBaseBackupAndWal(Path evidence) throws Exception {
        try (PostgreSQLContainer<?> source = new PostgreSQLContainer<>(POSTGRES_IMAGE)
                .withCommand("sh", "-ceu", """
                        mkdir -p /tmp/flowforge-archive /tmp/flowforge-base
                        chown postgres:postgres /tmp/flowforge-archive /tmp/flowforge-base
                        exec docker-entrypoint.sh postgres \
                          -c wal_level=replica \
                          -c archive_mode=on \
                          -c archive_command='test ! -f /tmp/flowforge-archive/%f && cp %p /tmp/flowforge-archive/%f'
                        """)) {
            source.start();
            Flyway.configure()
                    .dataSource(source.getJdbcUrl(), source.getUsername(), source.getPassword())
                    .locations("classpath:db/migration")
                    .load()
                    .migrate();
            JdbcClient jdbc = JdbcClient.create(new DriverManagerDataSource(
                    source.getJdbcUrl(), source.getUsername(), source.getPassword()
            ));
            jdbc.sql("CREATE TABLE pitr_marker(marker text PRIMARY KEY, created_at timestamptz NOT NULL)")
                    .update();

            exec(source, "create a SHA-256 base backup", "sh", "-ceu", """
                    rm -rf /tmp/flowforge-base/*
                    PGPASSWORD="$POSTGRES_PASSWORD" pg_basebackup \
                      --host=127.0.0.1 \
                      --username="$POSTGRES_USER" \
                      --pgdata=/tmp/flowforge-base \
                      --format=tar \
                      --wal-method=stream \
                      --checkpoint=fast \
                      --manifest-checksums=SHA256
                    rm -rf /tmp/flowforge-verify
                    mkdir -p /tmp/flowforge-verify/pg_wal
                    tar -xf /tmp/flowforge-base/base.tar -C /tmp/flowforge-verify
                    tar -xf /tmp/flowforge-base/pg_wal.tar -C /tmp/flowforge-verify/pg_wal
                    cp /tmp/flowforge-base/backup_manifest /tmp/flowforge-verify/backup_manifest
                    pg_verifybackup /tmp/flowforge-verify
                    rm -rf /tmp/flowforge-verify
                    """);

            jdbc.sql("""
                    INSERT INTO pitr_marker(marker, created_at)
                    VALUES ('keep-before-target', CURRENT_TIMESTAMP)
                    """).update();
            String restoreLsn = jdbc.sql("SELECT pg_create_restore_point(:name)::text")
                    .param("name", RESTORE_POINT)
                    .query(String.class)
                    .single();
            jdbc.sql("""
                    INSERT INTO pitr_marker(marker, created_at)
                    VALUES ('discard-after-target', CURRENT_TIMESTAMP)
                    """).update();
            String requiredWal = jdbc.sql("SELECT pg_walfile_name(pg_current_wal_lsn())")
                    .query(String.class)
                    .single();
            jdbc.sql("SELECT pg_switch_wal()::text").query(String.class).single();
            awaitArchivedWal(jdbc, requiredWal);

            source.copyFileFromContainer(
                    "/tmp/flowforge-base/base.tar", evidence.resolve("base.tar").toString()
            );
            source.copyFileFromContainer(
                    "/tmp/flowforge-base/pg_wal.tar", evidence.resolve("pg_wal.tar").toString()
            );
            source.copyFileFromContainer(
                    "/tmp/flowforge-base/backup_manifest", evidence.resolve("backup_manifest").toString()
            );
            Path archive = Files.createDirectories(evidence.resolve("archive"));
            List<String> archivedFiles = exec(source, "list archived WAL", "sh", "-ceu",
                    "ls -1 /tmp/flowforge-archive | sort")
                    .getStdout().lines().filter(name -> !name.isBlank()).toList();
            assertThat(archivedFiles).isNotEmpty();
            for (String archivedFile : archivedFiles) {
                assertThat(archivedFile).matches("[0-9A-F.]+(?:backup)?");
                source.copyFileFromContainer(
                        "/tmp/flowforge-archive/" + archivedFile,
                        archive.resolve(archivedFile).toString()
                );
            }
            Files.writeString(evidence.resolve("restore-point-lsn.txt"), restoreLsn);
        }
    }

    private static void restoreAndVerifyRecoveryPoint(Path evidence) throws Exception {
        try (GenericContainer<?> restored = new GenericContainer<>(POSTGRES_IMAGE)
                .withEnv("POSTGRES_DB", "test")
                .withEnv("POSTGRES_USER", "test")
                .withEnv("POSTGRES_PASSWORD", "test")
                .withExposedPorts(5432)
                .withCommand("sleep", "300")
                .waitingFor(Wait.forSuccessfulCommand("true"))) {
            restored.start();
            restored.copyFileToContainer(
                    Transferable.of(Files.readAllBytes(evidence.resolve("base.tar"))), "/tmp/base.tar"
            );
            restored.copyFileToContainer(
                    Transferable.of(Files.readAllBytes(evidence.resolve("pg_wal.tar"))), "/tmp/pg_wal.tar"
            );
            exec(restored, "create the restore archive directory", "mkdir", "-p", "/tmp/flowforge-archive");
            try (var archiveFiles = Files.list(evidence.resolve("archive"))) {
                for (Path archivedFile : archiveFiles.toList()) {
                    restored.copyFileToContainer(
                            Transferable.of(Files.readAllBytes(archivedFile)),
                            "/tmp/flowforge-archive/" + archivedFile.getFileName()
                    );
                }
            }
            exec(restored, "prepare the targeted recovery cluster", "sh", "-ceu", """
                    mkdir -p /tmp/flowforge-archive "$PGDATA" "$PGDATA/pg_wal"
                    tar -xf /tmp/base.tar -C "$PGDATA"
                    tar -xf /tmp/pg_wal.tar -C "$PGDATA/pg_wal"
                    cat >> "$PGDATA/postgresql.auto.conf" <<'EOF'
                    restore_command = 'cp /tmp/flowforge-archive/%f %p'
                    recovery_target_name = 'flowforge_before_loss'
                    recovery_target_action = 'promote'
                    EOF
                    touch "$PGDATA/recovery.signal"
                    chown -R postgres:postgres "$PGDATA" /tmp/flowforge-archive
                    chmod 0700 "$PGDATA"
                    docker-entrypoint.sh postgres > /tmp/flowforge-recovery.log 2>&1 &
                    """);

            String jdbcUrl = "jdbc:postgresql://%s:%d/test".formatted(
                    restored.getHost(), restored.getMappedPort(5432)
            );
            awaitPostgres(jdbcUrl, restored);
            Flyway.configure()
                    .dataSource(jdbcUrl, "test", "test")
                    .locations("classpath:db/migration")
                    .load()
                    .validate();
            JdbcClient jdbc = JdbcClient.create(new DriverManagerDataSource(jdbcUrl, "test", "test"));

            assertThat(jdbc.sql("SELECT marker FROM pitr_marker ORDER BY marker")
                    .query(String.class).list()).containsExactly("keep-before-target");
            assertThat(jdbc.sql("SELECT pg_is_in_recovery()")
                    .query(Boolean.class).single()).isFalse();
            assertThat(jdbc.sql("SELECT timeline_id FROM pg_control_checkpoint()")
                    .query(Integer.class).single()).isGreaterThan(1);
            assertThat(jdbc.sql("""
                    SELECT version FROM flyway_schema_history
                     WHERE success ORDER BY installed_rank DESC LIMIT 1
                    """).query(String.class).single()).isEqualTo("19");
        }
    }

    private static void awaitArchivedWal(JdbcClient jdbc, String requiredWal) throws Exception {
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            long failed = jdbc.sql("SELECT failed_count FROM pg_stat_archiver")
                    .query(Long.class).single();
            String lastArchived = jdbc.sql("SELECT COALESCE(last_archived_wal, '') FROM pg_stat_archiver")
                    .query(String.class).single();
            if (failed > 0) throw new AssertionError("PostgreSQL WAL archiving reported a failure");
            if (lastArchived.compareTo(requiredWal) >= 0) return;
            Thread.sleep(200);
        }
        throw new AssertionError("PostgreSQL did not archive the recovery WAL before the deadline");
    }

    private static void awaitPostgres(String jdbcUrl, GenericContainer<?> restored) throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        Exception lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            try (var ignored = DriverManager.getConnection(jdbcUrl, "test", "test")) {
                return;
            } catch (Exception failure) {
                lastFailure = failure;
                Thread.sleep(250);
            }
        }
        String log = exec(restored, "read recovery log", "sh", "-c",
                "cat /tmp/flowforge-recovery.log || true").getStdout();
        throw new AssertionError("Recovered PostgreSQL did not become ready:\n" + log, lastFailure);
    }

    private static org.testcontainers.containers.Container.ExecResult exec(
            GenericContainer<?> container,
            String description,
            String... command
    ) throws Exception {
        var result = container.execInContainer(command);
        assertThat(result.getExitCode())
                .as("%s failed: %s", description, result.getStderr())
                .isZero();
        return result;
    }
}
