package io.flowforge.domain.workflow;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskReliabilityPolicyTest {
    @Test
    void defaultsToOneAttemptForBackwardCompatibleExecution() {
        TaskReliabilityPolicy policy = TaskReliabilityPolicy.defaults();

        assertThat(policy.maxAttempts()).isEqualTo(1);
        assertThat(policy.initialBackoff()).isZero();
        assertThat(policy.attemptTimeout()).isNull();
        assertThat(policy.retryableErrorCodes()).isEmpty();
        assertThat(policy.isDefault()).isTrue();
    }

    @Test
    void normalizesAndProtectsRetryableErrorCodes() {
        Set<String> codes = new LinkedHashSet<>(Set.of(" timeout ", "gateway_unavailable"));
        TaskReliabilityPolicy policy = new TaskReliabilityPolicy(
                5,
                Duration.ofSeconds(1),
                2.0,
                Duration.ofMinutes(1),
                0.25,
                Duration.ofSeconds(30),
                codes
        );
        codes.add("LATE_MUTATION");

        assertThat(policy.retryableErrorCodes()).containsExactlyInAnyOrder("TIMEOUT", "GATEWAY_UNAVAILABLE");
        assertThat(policy.retryableErrorCodes()).doesNotContain("LATE_MUTATION");
    }

    @Test
    void rejectsUnsafePolicyBounds() {
        assertThatThrownBy(() -> new TaskReliabilityPolicy(
                0, Duration.ZERO, 2.0, Duration.ZERO, 0.0, null, Set.of()
        )).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxAttempts");

        assertThatThrownBy(() -> new TaskReliabilityPolicy(
                3, Duration.ofSeconds(5), 2.0, Duration.ofSeconds(1), 0.0, null, Set.of()
        )).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxBackoff");

        assertThatThrownBy(() -> new TaskReliabilityPolicy(
                1, Duration.ZERO, 2.0, Duration.ZERO, 1.1, null, Set.of()
        )).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("jitterFactor");

        assertThatThrownBy(() -> new TaskReliabilityPolicy(
                1, Duration.ZERO, 2.0, Duration.ZERO, 0.0, Duration.ZERO, Set.of()
        )).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("attemptTimeout");
    }
}
