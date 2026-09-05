package io.flowforge.controlplane.adapter.out.coordination;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RedisCoordinationKeyspaceTest {
    @Test
    void createsStableNamespacedKeysWithoutLeakingResourceNames() {
        RedisCoordinationKeyspace keyspace = new RedisCoordinationKeyspace("production-eu");

        String first = keyspace.permitKey("workflow/customer-sensitive-id");
        String duplicate = keyspace.permitKey("workflow/customer-sensitive-id");

        assertThat(first).isEqualTo(duplicate);
        assertThat(first).startsWith("flowforge:production-eu:coordination:{").endsWith("}");
        assertThat(first).doesNotContain("customer-sensitive-id");
    }

    @Test
    void rejectsUnsafeNamespaces() {
        assertThatThrownBy(() -> new RedisCoordinationKeyspace("production west"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
