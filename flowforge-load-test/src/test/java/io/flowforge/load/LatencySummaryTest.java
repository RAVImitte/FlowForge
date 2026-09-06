package io.flowforge.load;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LatencySummaryTest {
    @Test
    void computesNearestRankPercentilesInMilliseconds() {
        LatencySummary summary = LatencySummary.fromNanos(List.of(
                1_000_000L, 2_000_000L, 3_000_000L, 4_000_000L, 100_000_000L
        ));

        assertThat(summary.count()).isEqualTo(5);
        assertThat(summary.p50()).isEqualTo(3.0);
        assertThat(summary.p95()).isEqualTo(100.0);
        assertThat(summary.p99()).isEqualTo(100.0);
        assertThat(summary.max()).isEqualTo(100.0);
    }

    @Test
    void representsAnEmptyPopulationWithoutNan() {
        assertThat(LatencySummary.fromNanos(List.of()))
                .isEqualTo(new LatencySummary(0, 0, 0, 0, 0));
    }
}
