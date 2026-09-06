package io.flowforge.observability;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DeploymentAssetsTest {
    private final Path repositoryRoot = repositoryRoot();

    @Test
    void imagesUsePinnedBasesDeterministicInputsAndANonRootRuntime() throws IOException {
        for (String component : List.of("control-plane", "worker")) {
            String dockerfile = Files.readString(repositoryRoot.resolve("docker/" + component + ".Dockerfile"));

            assertThat(dockerfile)
                    .contains("eclipse-temurin:21-jdk-alpine@sha256:")
                    .contains("eclipse-temurin:21-jre-noble@sha256:")
                    .contains("ARG SOURCE_DATE_EPOCH=")
                    .contains("--mount=type=secret,id=build_truststore,required=false")
                    .contains("sed -i 's/\\r$//' mvnw")
                    .contains("-Dproject.build.outputTimestamp=\"${SOURCE_DATE_EPOCH}\"")
                    .contains("COPY --from=build --chown=10001:10001")
                    .contains("USER 10001:10001")
                    .contains("HEALTHCHECK")
                    .contains("/actuator/health/liveness")
                    .contains("ENTRYPOINT [\"java\", \"-jar\"")
                    .doesNotContain(":latest");
        }

        String builder = Files.readString(repositoryRoot.resolve("scripts/build-images.ps1"));
        assertThat(builder)
                .contains("git show -s --format=%ct HEAD")
                .contains("SOURCE_DATE_EPOCH=$sourceDateEpoch")
                .contains("-replace '^(https?://)[^/@]+@', '$1'")
                .contains("id=build_truststore,src=$resolvedTrustStore")
                .contains("--provenance=true")
                .contains("$user -ne \"10001:10001\"");
    }

    @Test
    void helmWorkloadsHaveSafeRolloutsProbesResourcesAndRestrictedContexts() throws IOException {
        for (String component : List.of("control-plane", "worker")) {
            String deployment = Files.readString(repositoryRoot.resolve(
                    "deploy/helm/flowforge/templates/" + component + "-deployment.yaml"));

            assertThat(deployment)
                    .contains("maxUnavailable: 0")
                    .contains("topologySpreadConstraints:")
                    .contains("kubernetes.io/hostname")
                    .contains("topology.kubernetes.io/zone")
                    .contains("flowforge.io/rotation-token:")
                    .contains(".Values.global.rotationToken | quote")
                    .contains("startupProbe:")
                    .contains("livenessProbe:")
                    .contains("readinessProbe:")
                    .contains("/actuator/health/liveness")
                    .contains("/actuator/health/readiness")
                    .contains("resources:")
                    .contains("runAsNonRoot: true")
                    .contains("readOnlyRootFilesystem: true")
                    .contains("allowPrivilegeEscalation: false")
                    .contains("drop: [\"ALL\"]")
                    .contains("automountServiceAccountToken: false")
                    .contains("seccompProfile:")
                    .contains("sizeLimit: 256Mi");
        }

        String budgets = Files.readString(repositoryRoot.resolve(
                "deploy/helm/flowforge/templates/poddisruptionbudgets.yaml"));
        String autoscalers = Files.readString(repositoryRoot.resolve(
                "deploy/helm/flowforge/templates/horizontalpodautoscalers.yaml"));
        assertThat(budgets).contains("kind: PodDisruptionBudget", "minAvailable:");
        assertThat(autoscalers).contains("apiVersion: autoscaling/v2", "stabilizationWindowSeconds: 300");
    }

    @Test
    void credentialsAndTrustMaterialAreOnlyConsumedThroughSecretReferences() throws IOException {
        Path templates = repositoryRoot.resolve("deploy/helm/flowforge/templates");
        String allTemplates;
        try (var files = Files.list(templates)) {
            allTemplates = files.filter(Files::isRegularFile)
                    .map(this::readUnchecked)
                    .reduce("", (left, right) -> left + "\n" + right);
        }

        assertThat(allTemplates)
                .doesNotContain("kind: Secret")
                .contains("secretKeyRef:")
                .contains("FLOWFORGE_DB_PASSWORD")
                .contains("SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG")
                .contains("FLOWFORGE_REDIS_PASSWORD")
                .contains("FLOWFORGE_OIDC_ISSUER_URI")
                .contains("FLOWFORGE_TRUSTSTORE_PASSWORD")
                .contains("secretName: {{ .Values.trustStore.secretName }}");

        String readme = Files.readString(repositoryRoot.resolve("deploy/helm/flowforge/README.md"));
        assertThat(readme)
                .contains("does not create or accept credential values")
                .contains("flowforge-database")
                .contains("flowforge-kafka")
                .contains("flowforge-redis")
                .contains("flowforge-oidc")
                .contains("flowforge-truststore")
                .contains("global.rotationToken")
                .contains("two-valid-credential overlap")
                .contains("image digests");

        String values = Files.readString(repositoryRoot.resolve("deploy/helm/flowforge/values.yaml"));
        assertThat(values).contains("rotationToken: \"\"");
    }

    private String readUnchecked(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException exception) {
            throw new IllegalStateException("Could not read " + path, exception);
        }
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
