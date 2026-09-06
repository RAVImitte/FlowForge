package io.flowforge.observability;

import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class DeliveryPipelineAssetsTest {
    private static final Pattern ACTION_REFERENCE = Pattern.compile(
            "(?m)^\\s*uses:\\s*([^@\\s]+)@([^\\s#]+)"
    );

    private final Path repositoryRoot = repositoryRoot();

    @Test
    void pullRequestPipelineHasIndependentRepeatableQualityGates() throws IOException {
        String workflow = read(".github/workflows/ci.yml");
        assertThat(parseYaml(workflow)).containsKeys("name", "on", "permissions", "jobs");

        assertThat(workflow)
                .contains("permissions:\n  contents: read")
                .contains("cancel-in-progress: true")
                .contains("-Gate Fast")
                .contains("-Gate Architecture")
                .contains("-Gate Security")
                .contains("-Gate Upgrade")
                .contains("-Gate Recovery")
                .contains("-Gate Full")
                .contains("-Gate Manifests")
                .contains("-Gate Sbom")
                .contains("scan-type: fs")
                .contains("scanners: vuln,secret,misconfig")
                .contains("version: v0.74.0")
                .contains("severity: HIGH,CRITICAL")
                .contains("./scripts/build-images.ps1")
                .contains("flowforge-ci/control-plane:")
                .contains("flowforge-ci/worker:")
                .doesNotContain("pull_request_target");

        assertActionsAreCommitPinned(workflow);
    }

    @Test
    void releasePipelineBlocksPublicationUntilUpgradeSmokeAndSupplyChainChecksPass() throws IOException {
        String workflow = read(".github/workflows/release.yml");
        assertThat(parseYaml(workflow)).containsKeys("name", "on", "permissions", "jobs");

        assertThat(workflow)
                .contains("workflow_dispatch:")
                .contains("environment: release")
                .contains("cancel-in-progress: false")
                .contains("test \"$GITHUB_REF\" = \"refs/heads/main\"")
                .contains("-Gate Security")
                .contains("-Gate Upgrade")
                .contains("-Gate Recovery")
                .contains("-Gate Full")
                .contains("-Gate Manifests")
                .contains("run-load-topology.ps1 -Topology single-6p -Profile smoke -ResetData")
                .contains("-Gate Sbom")
                .contains("provenance: mode=max")
                .contains("sbom: true")
                .contains("severity: HIGH,CRITICAL")
                .contains("version: v0.74.0")
                .contains("cosign-release: v2.6.5")
                .contains("cosign sign --yes")
                .contains("cosign attest --yes")
                .contains("cosign verify --key")
                .contains("cosign verify-attestation --key")
                .contains("REGISTRY_USERNAME")
                .contains("REGISTRY_PASSWORD")
                .contains("COSIGN_PRIVATE_KEY")
                .doesNotContain("pull_request:", "pull_request_target");

        assertActionsAreCommitPinned(workflow);
    }

    @Test
    void localQualityGateDefinesFastFullArchitectureMigrationManifestAndSbomContracts() throws IOException {
        String script = read("scripts/invoke-quality-gate.ps1");
        String pom = read("pom.xml");

        assertThat(script)
                .contains("ValidateSet(\"Fast\", \"Architecture\", \"Security\", \"Migration\", \"Upgrade\", \"Recovery\", \"Full\", \"Manifests\", \"Sbom\")")
                .contains("mvnw.cmd")
                .contains("Join-Path $repositoryRoot \"mvnw\"")
                .contains("-Dtest=ArchitectureTest")
                .contains("SecurityConfigurationTest,FlowForgeJwtAuthenticationConverterTest,TenantContextFilterTest")
                .contains("-Dtest=TenantMigrationIntegrationTest")
                .contains("TenantMigrationIntegrationTest,SchemaRollbackCompatibilityIntegrationTest")
                .contains("PostgresBackupRestoreIntegrationTest,PostgresPointInTimeRecoveryIntegrationTest,")
                .contains("SchemaRollbackCompatibilityIntegrationTest,CoordinationIntegrationTest,DeadLetterReplayIntegrationTest")
                .contains("helm\" @(")
                .contains("global.rotationToken=manifest-validation")
                .contains("kubectl\" @(")
                .contains("target/sbom/flowforge.cdx.json");

        assertThat(pom)
                .contains("<id>fast</id>")
                .contains("<exclude>**/*IntegrationTest.java</exclude>")
                .contains("<exclude>**/WorkerApplicationTest.java</exclude>")
                .contains("<id>sbom</id>")
                .contains("<artifactId>cyclonedx-maven-plugin</artifactId>")
                .contains("<goal>makeAggregateBom</goal>")
                .contains("<schemaVersion>1.6</schemaVersion>")
                .contains("<includeTestScope>false</includeTestScope>");
    }

    @Test
    void releaseSmokeHarnessCanSelectTheNativeMavenWrapperAndHideWindowsProcessesOnly() throws IOException {
        String script = read("scripts/run-load-topology.ps1");

        assertThat(script)
                .contains("[Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT")
                .contains("if ($isWindowsHost) { \".\\mvnw.cmd\" } else { \"./mvnw\" }")
                .contains("if ($isWindowsHost) { $parameters.WindowStyle = \"Hidden\" }")
                .contains("Start-BackgroundProcess -FilePath \"java\"")
                .contains("Start-BackgroundProcess -FilePath $loadProcessHost");
    }

    @Test
    void recoveryRunbookPreservesTheAuthoritativeAndEphemeralStateBoundaries() throws IOException {
        String runbook = read("docs/operations/phase-7-recovery-runbook.md");
        String recoveryTest = read("flowforge-control-plane/src/test/java/io/flowforge/controlplane/"
                + "PostgresBackupRestoreIntegrationTest.java");
        String pitrTest = read("flowforge-control-plane/src/test/java/io/flowforge/controlplane/"
                + "PostgresPointInTimeRecoveryIntegrationTest.java");
        String rollbackTest = read("flowforge-control-plane/src/test/java/io/flowforge/controlplane/"
                + "SchemaRollbackCompatibilityIntegrationTest.java");
        String upgradeRunbook = read("docs/operations/phase-7-upgrade-and-credential-rotation-runbook.md");

        assertThat(runbook)
                .contains("PostgreSQL is the durable correctness boundary")
                .contains("Never restore Redis from a backup")
                .contains("Do not attach the restored applications to the old Kafka transport")
                .contains("pg_dump --format=custom")
                .contains("pg_restore --single-transaction --exit-on-error")
                .contains("-Gate Recovery")
                .contains("recovery_target_name")
                .contains("recovery_target_action = 'promote'");
        assertThat(recoveryTest)
                .contains("--format=custom")
                .contains("--single-transaction")
                .contains("tableCounts(restored)")
                .contains("Flyway.configure()")
                .contains("append-only")
                .contains("recovery-after-snapshot");
        assertThat(pitrTest)
                .contains("pg_basebackup")
                .contains("pg_verifybackup")
                .contains("pg_create_restore_point")
                .contains("recovery.signal")
                .contains("recovery_target_name");
        assertThat(rollbackTest)
                .contains(".target(\"17\")")
                .contains("insertUsingV17Contract")
                .contains("secret_references::text");
        assertThat(upgradeRunbook)
                .contains("forward-only")
                .contains("two-valid-credential overlap")
                .contains("global.rotationToken")
                .contains("Never put secret material in the rotation token");
    }

    @Test
    void incidentRunbookRequiresBoundedTenantSafeAndIdempotentReplay() throws IOException {
        String runbook = read("docs/operations/phase-7-incident-and-dlq-replay-runbook.md");
        String migration = read("flowforge-control-plane/src/main/resources/db/migration/"
                + "V19__add_dlq_replay_ledger.sql");
        String acceptance = read("flowforge-control-plane/src/test/java/io/flowforge/controlplane/"
                + "DeadLetterReplayIntegrationTest.java");

        assertThat(runbook)
                .contains("exactly one retained record")
                .contains("foreign-tenant record is returned as `404`")
                .contains("never returns the raw message value")
                .contains("Never script an unbounded partition or topic replay")
                .contains("at-least-once ambiguity")
                .contains("Idempotency-Key")
                .contains("-Gate Recovery");
        assertThat(migration)
                .contains("dlq_replay_request")
                .contains("UNIQUE (tenant_id, dlq_topic, dlq_partition, dlq_offset)")
                .doesNotContain("payload", "message_value");
        assertThat(acceptance)
                .contains("replaysOneTenantOwnedRecordOnceAndPersistsOnlyOperationalEvidence")
                .contains("ALREADY_PUBLISHED")
                .contains("column_name IN ('payload', 'record_key', 'headers')");
    }

    @Test
    void phaseSevenCloseoutHasAnExecutableSameRevisionAcceptanceBoundary() throws IOException {
        String orchestrator = read("scripts/invoke-release-acceptance.ps1");
        String matrix = read("docs/operations/phase-7-release-acceptance.md");
        String decision = read("docs/adr/007-security-tenancy-and-operational-release-boundary.md");

        assertThat(orchestrator)
                .contains("$ErrorActionPreference = \"Stop\"")
                .contains("@(", "\"Security\"", "\"Upgrade\"", "\"Recovery\"", "\"Full\"", "\"Manifests\"", "\"Sbom\"")
                .contains("invoke-quality-gate.ps1")
                .contains("$LASTEXITCODE -ne 0")
                .contains("Release acceptance passed:");
        assertThat(matrix)
                .contains("every repository gate passes on the same revision")
                .contains("Passing this matrix does not itself publish")
                .contains("Protected publication matrix")
                .contains("No fixed HIGH or CRITICAL finding remains unhandled")
                .contains("Publication controls remain intentionally unexecuted");
        assertThat(decision)
                .contains("# ADR-007:")
                .contains("## Status\n\nAccepted")
                .contains("Authentication alone does not prove tenant isolation")
                .contains("PostgreSQL is authoritative")
                .contains("protected release workflow alone may publish")
                .contains("local acceptance run cannot certify");
    }

    private void assertActionsAreCommitPinned(String workflow) {
        Matcher matcher = ACTION_REFERENCE.matcher(workflow);
        int references = 0;
        while (matcher.find()) {
            references++;
            assertThat(matcher.group(2))
                    .as("action %s must use an immutable commit", matcher.group(1))
                    .matches("[0-9a-f]{40}");
        }
        assertThat(references).isGreaterThan(0);
    }

    private String read(String relativePath) throws IOException {
        return Files.readString(repositoryRoot.resolve(relativePath)).replace("\r\n", "\n");
    }

    @SuppressWarnings("unchecked")
    private static java.util.Map<String, Object> parseYaml(String yaml) {
        return (java.util.Map<String, Object>) new Yaml(new SafeConstructor(new LoaderOptions())).load(yaml);
    }

    private static Path repositoryRoot() {
        Path current = Path.of("").toAbsolutePath();
        while (current != null && !Files.isRegularFile(current.resolve("mvnw.cmd"))) {
            current = current.getParent();
        }
        if (current == null) throw new IllegalStateException("Could not locate FlowForge repository root");
        return current;
    }
}
