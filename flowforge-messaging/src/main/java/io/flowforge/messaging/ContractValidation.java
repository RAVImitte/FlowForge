package io.flowforge.messaging;

import java.util.LinkedHashMap;
import java.util.Map;

final class ContractValidation {
    private ContractValidation() {
    }

    static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(field + " must not be blank");
        return value.trim();
    }

    static Map<String, Object> immutableConfiguration(Map<String, Object> value) {
        return value == null ? Map.of() : Map.copyOf(new LinkedHashMap<>(value));
    }
}
