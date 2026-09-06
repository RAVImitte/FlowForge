package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.domain.workflow.SecretReference;
import io.flowforge.domain.workflow.TaskReliabilityPolicy;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

public record WorkflowRequest(
        @NotBlank @Size(max = 200) String name,
        @Size(max = 2000) String description,
        @NotEmpty @Size(max = 1000) List<@Valid TaskRequest> tasks,
        List<@Valid DependencyRequest> dependencies,
        @Min(1) @Max(100000) Integer maxConcurrentExecutions
) {
    public record TaskRequest(
            @NotBlank @Pattern(regexp = "[A-Z][A-Z0-9_]{0,99}") String key,
            @NotBlank @Size(max = 200) String name,
            @NotBlank @Pattern(regexp = "[A-Z][A-Z0-9_.-]{0,99}") String type,
            Map<String, Object> configuration,
            @Size(max = 100) Map<
                    @Pattern(regexp = "[A-Za-z][A-Za-z0-9_.-]{0,99}") String,
                    @Valid SecretReferenceRequest> secretReferences,
            @Valid ReliabilityPolicyRequest reliabilityPolicy,
            @Min(1) @Max(100000) Integer maxConcurrency
    ) {
        public TaskRequest(
                String key,
                String name,
                String type,
                Map<String, Object> configuration,
                ReliabilityPolicyRequest reliabilityPolicy,
                Integer maxConcurrency
        ) {
            this(key, name, type, configuration, Map.of(), reliabilityPolicy, maxConcurrency);
        }
    }

    public record SecretReferenceRequest(
            @NotBlank @Pattern(regexp = "[a-z][a-z0-9.-]{0,63}") String provider,
            @NotBlank @Size(max = 512) String name,
            @Size(max = 200) String version
    ) {
        SecretReference toDomain() {
            return new SecretReference(provider, name, version);
        }
    }

    public record ReliabilityPolicyRequest(
            @Min(1) @Max(100) Integer maxAttempts,
            @Min(0) Long initialBackoffMs,
            @DecimalMin("1.0") Double backoffMultiplier,
            @Min(0) Long maxBackoffMs,
            @DecimalMin("0.0") @DecimalMax("1.0") Double jitterFactor,
            @Positive Long attemptTimeoutMs,
            @Size(max = 100) Set<@NotBlank @Size(max = 100) String> retryableErrorCodes
    ) {
        TaskReliabilityPolicy toDomain() {
            long initial = initialBackoffMs == null ? 0 : initialBackoffMs;
            return new TaskReliabilityPolicy(
                    maxAttempts == null ? 1 : maxAttempts,
                    Duration.ofMillis(initial),
                    backoffMultiplier == null ? 2.0 : backoffMultiplier,
                    Duration.ofMillis(maxBackoffMs == null ? initial : maxBackoffMs),
                    jitterFactor == null ? 0.0 : jitterFactor,
                    attemptTimeoutMs == null ? null : Duration.ofMillis(attemptTimeoutMs),
                    retryableErrorCodes == null ? Set.of() : retryableErrorCodes
            );
        }
    }

    public record DependencyRequest(
            @NotBlank String taskKey,
            @NotBlank String dependsOnTaskKey
    ) {
    }
}
