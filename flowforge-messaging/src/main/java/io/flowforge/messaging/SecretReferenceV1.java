package io.flowforge.messaging;

import java.util.Locale;

public record SecretReferenceV1(String provider, String name, String version) {
    public SecretReferenceV1 {
        provider = ContractValidation.requireNonBlank(provider, "provider").toLowerCase(Locale.ROOT);
        name = ContractValidation.requireNonBlank(name, "name");
        version = version == null || version.isBlank() ? null : version.strip();
        if (!provider.matches("[a-z][a-z0-9.-]{0,63}")) {
            throw new IllegalArgumentException("provider must be a lowercase provider identifier");
        }
        if (name.length() > 512) throw new IllegalArgumentException("name cannot exceed 512 characters");
        if (version != null && version.length() > 200) {
            throw new IllegalArgumentException("version cannot exceed 200 characters");
        }
    }
}
