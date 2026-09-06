package io.flowforge.observability;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SensitiveDataRedactorTest {
    @Test
    void redactsResolvedValuesAndCredentialAssignments() {
        String redacted = SensitiveDataRedactor.redact(
                "provider failed password=hunter2 and response contained exact-material",
                List.of("exact-material")
        );

        assertThat(redacted)
                .doesNotContain("hunter2", "exact-material")
                .contains("password=[REDACTED]")
                .contains(SensitiveDataRedactor.REDACTED);
    }
}
