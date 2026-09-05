package io.flowforge.observability;

import java.util.UUID;
import java.util.regex.Pattern;

public final class CorrelationIds {
    public static final int MAXIMUM_LENGTH = 128;
    private static final Pattern SAFE_VALUE = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}");

    private CorrelationIds() {
    }

    public static String acceptOrGenerate(String candidate) {
        if (candidate != null && SAFE_VALUE.matcher(candidate).matches()) return candidate;
        return UUID.randomUUID().toString();
    }

    public static boolean isSafe(String candidate) {
        return candidate != null && SAFE_VALUE.matcher(candidate).matches();
    }
}
