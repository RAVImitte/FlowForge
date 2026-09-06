package io.flowforge.observability;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

public final class SensitiveDataRedactor {
    public static final String REDACTED = "[REDACTED]";
    private static final Pattern ASSIGNMENT = Pattern.compile(
            "(?i)(password|secret|credential|private[-_]?key|api[-_]?key|access[-_]?token|refresh[-_]?token|token)"
                    + "(\\s*[:=]\\s*)([^\\s,;]+)"
    );

    private SensitiveDataRedactor() {
    }

    public static String redact(String text, Collection<String> secretValues) {
        if (text == null) return null;
        String redacted = text;
        List<String> values = secretValues == null
                ? List.of()
                : secretValues.stream()
                        .filter(value -> value != null && !value.isEmpty())
                        .distinct()
                        .sorted(Comparator.comparingInt(String::length).reversed())
                        .toList();
        for (String value : values) redacted = redacted.replace(value, REDACTED);
        return ASSIGNMENT.matcher(redacted).replaceAll("$1$2" + REDACTED);
    }

    public static String redact(String text) {
        return redact(text, List.of());
    }
}
