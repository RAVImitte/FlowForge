package io.flowforge.domain.tenancy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TenantIdTest {
    @Test
    void acceptsAndTrimsBoundedDnsLabels() {
        TenantId tenantId = new TenantId("  orders-eu1  ");

        assertThat(tenantId.value()).isEqualTo("orders-eu1");
        assertThat(tenantId).hasToString("orders-eu1");
    }

    @Test
    void rejectsAmbiguousOrUnsafeIdentifiers() {
        assertThatThrownBy(() -> new TenantId("Orders"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TenantId("orders_eu"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TenantId("-orders"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TenantId("orders-"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMissingAndOversizedIdentifiers() {
        assertThatThrownBy(() -> new TenantId(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TenantId("a".repeat(64)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
