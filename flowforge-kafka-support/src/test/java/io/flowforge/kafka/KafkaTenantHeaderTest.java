package io.flowforge.kafka;

import io.flowforge.messaging.FlowForgeHeaders;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaTenantHeaderTest {
    @Test
    void writesAndRequiresOneMatchingTenantHeader() {
        RecordHeaders headers = new RecordHeaders();

        KafkaTenantHeader.add(headers, "merchant-a");
        KafkaTenantHeader.requireMatching(headers, "merchant-a");

        assertThat(new String(
                headers.lastHeader(FlowForgeHeaders.TENANT_ID).value(), StandardCharsets.UTF_8
        )).isEqualTo("merchant-a");
        assertThatThrownBy(() -> KafkaTenantHeader.requireMatching(headers, "merchant-b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match");
    }

    @Test
    void allowsOnlyHeaderlessLegacyLocalRecords() {
        RecordHeaders headers = new RecordHeaders();

        KafkaTenantHeader.requireMatching(headers, "local");

        assertThatThrownBy(() -> KafkaTenantHeader.requireMatching(headers, "merchant-a"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing");
    }

    @Test
    void rejectsDuplicateTenantHeaders() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(FlowForgeHeaders.TENANT_ID, "merchant-a".getBytes(StandardCharsets.UTF_8));
        headers.add(FlowForgeHeaders.TENANT_ID, "merchant-a".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> KafkaTenantHeader.requireMatching(headers, "merchant-a"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
    }
}
