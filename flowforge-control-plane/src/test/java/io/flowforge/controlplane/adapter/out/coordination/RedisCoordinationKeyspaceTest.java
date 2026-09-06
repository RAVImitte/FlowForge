package io.flowforge.controlplane.adapter.out.coordination;

import io.flowforge.domain.tenancy.TenantId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RedisCoordinationKeyspaceTest {
    @Test
    void createsStableNamespacedKeysWithoutLeakingResourceNames() {
        RedisCoordinationKeyspace keyspace = new RedisCoordinationKeyspace("production-eu");

        TenantId tenantId = new TenantId("merchant-a");
        String first = keyspace.permitKey(tenantId, "workflow/customer-sensitive-id");
        String duplicate = keyspace.permitKey(tenantId, "workflow/customer-sensitive-id");

        assertThat(first).isEqualTo(duplicate);
        assertThat(first)
                .startsWith("flowforge:production-eu:tenant:merchant-a:coordination:{")
                .endsWith("}");
        assertThat(first).doesNotContain("customer-sensitive-id");
        assertThat(first).contains(":tenant:merchant-a:");
        assertThat(keyspace.permitKey(new TenantId("merchant-b"), "workflow/customer-sensitive-id"))
                .isNotEqualTo(first);
    }

    @Test
    void rejectsUnsafeNamespaces() {
        assertThatThrownBy(() -> new RedisCoordinationKeyspace("production west"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
