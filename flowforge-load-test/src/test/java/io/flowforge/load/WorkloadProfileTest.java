package io.flowforge.load;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkloadProfileTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void loadsAValidatedVersionedProfile() throws Exception {
        Path profilePath = temporaryDirectory.resolve("profile.json");
        Files.writeString(profilePath, profileJson("API", 4));

        WorkloadProfile profile = WorkloadProfile.load(profilePath, new ObjectMapper());

        assertThat(profile.name()).isEqualTo("test");
        assertThat(profile.mode()).isEqualTo(WorkloadProfile.Mode.API);
        assertThat(profile.baseUrls()).containsExactly("http://localhost:8080");
        assertThat(profile.operations()).isEqualTo(4);
        assertThat(profile.taskMaxAttempts()).isEqualTo(1);
        assertThat(profile.retryBackoffMs()).isZero();
        assertThat(profile.workflowMaxConcurrency()).isNull();
        assertThat(profile.taskMaxConcurrency()).isNull();
        assertThat(profile.thresholds().minimumSuccessRatio()).isEqualTo(1.0);
    }

    @Test
    void rejectsUnsupportedSchemasBeforeSendingTraffic() throws Exception {
        Path profilePath = temporaryDirectory.resolve("profile.json");
        Files.writeString(profilePath, profileJson("API", 4).replace("\"schemaVersion\": 1", "\"schemaVersion\": 2"));

        assertThatThrownBy(() -> WorkloadProfile.load(profilePath, new ObjectMapper()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schemaVersion");
    }

    @Test
    void loadsAndNormalizesMultipleControlPlaneOrigins() throws Exception {
        Path profilePath = temporaryDirectory.resolve("profile.json");
        Files.writeString(profilePath, profileJson("API", 4).replace(
                "\"baseUrl\": \"http://localhost:8080/\"",
                "\"baseUrls\": [\"http://localhost:8080/\", \"http://localhost:8082\", \"http://localhost:8080\"]"
        ));

        WorkloadProfile profile = WorkloadProfile.load(profilePath, new ObjectMapper());

        assertThat(profile.baseUrls()).containsExactly("http://localhost:8080", "http://localhost:8082");
        assertThat(profile.primaryBaseUrl()).isEqualTo("http://localhost:8080");
    }

    @Test
    void loadsExplicitTaskRetryPolicyForResilienceTraffic() throws Exception {
        Path profilePath = temporaryDirectory.resolve("profile.json");
        Files.writeString(profilePath, profileJson("API", 4).replace(
                "\"taskDelayMs\": 0,",
                "\"taskDelayMs\": 0,\n  \"taskMaxAttempts\": 3,\n  \"retryBackoffMs\": 500,"
        ));

        WorkloadProfile profile = WorkloadProfile.load(profilePath, new ObjectMapper());

        assertThat(profile.taskMaxAttempts()).isEqualTo(3);
        assertThat(profile.retryBackoffMs()).isEqualTo(500);
    }

    @Test
    void loadsExplicitConcurrencyLimitsForCoordinationTraffic() throws Exception {
        Path profilePath = temporaryDirectory.resolve("profile.json");
        Files.writeString(profilePath, profileJson("API", 4).replace(
                "\"taskDelayMs\": 0,",
                "\"taskDelayMs\": 0,\n  \"workflowMaxConcurrency\": 10,\n  \"taskMaxConcurrency\": 2,"
        ));

        WorkloadProfile profile = WorkloadProfile.load(profilePath, new ObjectMapper());

        assertThat(profile.workflowMaxConcurrency()).isEqualTo(10);
        assertThat(profile.taskMaxConcurrency()).isEqualTo(2);
    }

    static String profileJson(String mode, int operations) {
        return """
                {
                  "schemaVersion": 1,
                  "name": "test",
                  "mode": "%s",
                  "baseUrl": "http://localhost:8080/",
                  "operations": %d,
                  "arrivalRatePerSecond": 1000,
                  "maxInFlight": 4,
                  "fanOut": 3,
                  "taskDelayMs": 0,
                  "pollIntervalMs": 1,
                  "operationTimeoutSeconds": 2,
                  "scheduleLeadSeconds": 1,
                  "thresholds": {
                    "minimumAcceptanceRatio": 1.0,
                    "minimumSuccessRatio": 1.0,
                    "maximumErrorRatio": 0.0,
                    "maximumStartP95Ms": 1000,
                    "maximumCompletionP95Ms": 1000
                  }
                }
                """.formatted(mode, operations);
    }
}
