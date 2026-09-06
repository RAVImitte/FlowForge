package io.flowforge.domain.tenancy;

import java.util.regex.Pattern;

public record TenantId(String value) {
    private static final int MAX_LENGTH = 63;
    private static final Pattern VALID = Pattern.compile("[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?");
    public static final TenantId LOCAL = new TenantId("local");

    public TenantId {
        if (value == null) {
            throw new IllegalArgumentException("Tenant ID is required");
        }
        value = value.strip();
        if (value.length() > MAX_LENGTH || !VALID.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "Tenant ID must be a lowercase DNS label of at most 63 characters"
            );
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
