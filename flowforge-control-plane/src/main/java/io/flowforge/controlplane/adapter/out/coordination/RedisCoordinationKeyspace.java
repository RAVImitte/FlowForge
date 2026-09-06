package io.flowforge.controlplane.adapter.out.coordination;

import io.flowforge.domain.tenancy.TenantId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

@Component
public class RedisCoordinationKeyspace {
    private final String namespace;

    public RedisCoordinationKeyspace(
            @Value("${flowforge.coordination.namespace:local}") String namespace
    ) {
        if (namespace == null || !namespace.matches("[A-Za-z0-9._-]{1,50}")) {
            throw new IllegalArgumentException(
                    "Coordination namespace must contain 1-50 letters, digits, dots, underscores, or hyphens"
            );
        }
        this.namespace = namespace;
    }

    public String permitKey(TenantId tenantId, String resourceKey) {
        if (tenantId == null) throw new IllegalArgumentException("tenantId must not be null");
        if (resourceKey == null || resourceKey.isBlank()) {
            throw new IllegalArgumentException("resourceKey must not be blank");
        }
        return "flowforge:" + namespace + ":tenant:" + tenantId.value()
                + ":coordination:{" + sha256(tenantId.value() + ":" + resourceKey.strip()) + "}";
    }

    public String rateLimitKey(TenantId tenantId, String bucketKey) {
        if (tenantId == null) throw new IllegalArgumentException("tenantId must not be null");
        if (bucketKey == null || bucketKey.isBlank()) {
            throw new IllegalArgumentException("bucketKey must not be blank");
        }
        return "flowforge:" + namespace + ":tenant:" + tenantId.value()
                + ":rate-limit:{" + sha256(tenantId.value() + ":" + bucketKey.strip()) + "}";
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
