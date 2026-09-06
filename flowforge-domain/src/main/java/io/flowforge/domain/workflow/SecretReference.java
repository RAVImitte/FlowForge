package io.flowforge.domain.workflow;

import java.util.Locale;

public record SecretReference(String provider, String name, String version) {
    public SecretReference {
        provider = require(provider, "provider").toLowerCase(Locale.ROOT);
        name = require(name, "name");
        version = normalize(version);
        if (!provider.matches("[a-z][a-z0-9.-]{0,63}")) {
            throw new IllegalArgumentException("secret provider must be a lowercase provider identifier");
        }
        if (name.length() > 512) throw new IllegalArgumentException("secret name cannot exceed 512 characters");
        if (version != null && version.length() > 200) {
            throw new IllegalArgumentException("secret version cannot exceed 200 characters");
        }
    }

    private static String require(String value, String field) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("secret " + field + " must not be blank");
        return value.strip();
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.strip();
    }
}
