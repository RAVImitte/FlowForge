package io.flowforge.kafka;

import io.flowforge.messaging.FlowForgeHeaders;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;

public final class KafkaTenantHeader {
    private KafkaTenantHeader() {
    }

    public static void add(Headers headers, String tenantId) {
        if (headers.lastHeader(FlowForgeHeaders.TENANT_ID) != null) {
            throw new IllegalArgumentException("Kafka record already contains a tenant header");
        }
        headers.add(FlowForgeHeaders.TENANT_ID, tenantId.getBytes(StandardCharsets.UTF_8));
    }

    public static void requireMatching(Headers headers, String tenantId) {
        Header header = headers.lastHeader(FlowForgeHeaders.TENANT_ID);
        if (header == null) {
            if ("local".equals(tenantId)) return;
            throw new IllegalArgumentException("Kafka record is missing its tenant header");
        }
        String headerTenant = new String(header.value(), StandardCharsets.UTF_8);
        if (!tenantId.equals(headerTenant)) {
            throw new IllegalArgumentException("Kafka tenant header does not match the message envelope");
        }
        int occurrences = 0;
        for (Header ignored : headers.headers(FlowForgeHeaders.TENANT_ID)) occurrences++;
        if (occurrences != 1) {
            throw new IllegalArgumentException("Kafka record must contain exactly one tenant header");
        }
    }
}
