package io.flowforge.messaging;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

final class ContractValidation {
    private static final Pattern TENANT_ID = Pattern.compile(
            "[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    );

    private ContractValidation() {
    }

    static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(field + " must not be blank");
        return value.trim();
    }

    static Map<String, Object> immutableConfiguration(Map<String, Object> value) {
        return value == null ? Map.of() : Map.copyOf(new LinkedHashMap<>(value));
    }

    static String requireTenantId(String value) {
        String tenantId = requireNonBlank(value, "tenantId");
        if (!TENANT_ID.matcher(tenantId).matches()) {
            throw new IllegalArgumentException("tenantId must be a lowercase DNS label of at most 63 characters");
        }
        return tenantId;
    }
}
