package io.flowforge.observability;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CorrelationIdsTest {
    @Test
    void preservesSafeCallerSuppliedIdentifiers() {
        assertThat(CorrelationIds.acceptOrGenerate("checkout_01J.test:2"))
                .isEqualTo("checkout_01J.test:2");
    }

    @Test
    void replacesMissingOversizedOrLogInjectingIdentifiers() {
        assertThat(CorrelationIds.isSafe(CorrelationIds.acceptOrGenerate(null))).isTrue();
        assertThat(CorrelationIds.isSafe(CorrelationIds.acceptOrGenerate("line\nbreak"))).isTrue();
        assertThat(CorrelationIds.isSafe(CorrelationIds.acceptOrGenerate("x".repeat(129)))).isTrue();
    }
}
