package io.flowforge.domain.execution;

import io.flowforge.domain.workflow.TaskReliabilityPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class RetryBackoffTest {
    private static final UUID TASK_ID = UUID.fromString("30000000-0000-0000-0000-000000000001");

    @Test
    void growsExponentiallyAndCapsBeforeJitter() {
        TaskReliabilityPolicy policy = policy(0.0);

        assertThat(RetryBackoff.delayAfter(policy, TASK_ID, 1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(RetryBackoff.delayAfter(policy, TASK_ID, 2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(RetryBackoff.delayAfter(policy, TASK_ID, 3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(RetryBackoff.delayAfter(policy, TASK_ID, 4)).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void jitterIsBoundedAndDeterministicAcrossRestarts() {
        TaskReliabilityPolicy policy = policy(0.25);

        Duration first = RetryBackoff.delayAfter(policy, TASK_ID, 3);
        Duration replay = RetryBackoff.delayAfter(policy, TASK_ID, 3);

        assertThat(replay).isEqualTo(first);
        assertThat(first).isBetween(Duration.ofSeconds(3), Duration.ofSeconds(5));
    }

    @Test
    void policyUsesAllowListBeforeWorkerClassification() {
        TaskReliabilityPolicy allowListed = new TaskReliabilityPolicy(
                3, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(5), 0.0, null, Set.of("TIMEOUT")
        );
        TaskReliabilityPolicy workerClassified = new TaskReliabilityPolicy(
                3, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(5), 0.0, null, Set.of()
        );

        assertThat(allowListed.isRetryable("timeout", false)).isTrue();
        assertThat(allowListed.isRetryable("BAD_REQUEST", true)).isFalse();
        assertThat(workerClassified.isRetryable("ANY", true)).isTrue();
        assertThat(workerClassified.isRetryable("ANY", null)).isFalse();
    }

    private static TaskReliabilityPolicy policy(double jitter) {
        return new TaskReliabilityPolicy(
                10,
                Duration.ofSeconds(1),
                2.0,
                Duration.ofSeconds(5),
                jitter,
                null,
                Set.of()
        );
    }
}
