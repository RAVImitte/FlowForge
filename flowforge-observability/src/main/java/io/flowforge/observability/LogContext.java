package io.flowforge.observability;

import org.slf4j.MDC;

import java.util.LinkedHashMap;
import java.util.Map;

public final class LogContext implements AutoCloseable {
    private final Map<String, String> previous;
    private boolean closed;

    private LogContext(Map<String, ?> attributes) {
        attributes.keySet().forEach(key -> {
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("Log context keys must not be blank");
            }
        });
        previous = MDC.getCopyOfContextMap();
        attributes.forEach((key, value) -> {
            if (value != null) MDC.put(key, String.valueOf(value));
        });
    }

    public static LogContext open(Object... keyValues) {
        if (keyValues == null || keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("Log context requires key/value pairs");
        }
        Map<String, Object> attributes = new LinkedHashMap<>();
        for (int index = 0; index < keyValues.length; index += 2) {
            Object key = keyValues[index];
            if (!(key instanceof String name)) {
                throw new IllegalArgumentException("Log context keys must be strings");
            }
            attributes.put(name, keyValues[index + 1]);
        }
        return new LogContext(attributes);
    }

    @Override
    public void close() {
        if (closed) return;
        if (previous == null) MDC.clear(); else MDC.setContextMap(previous);
        closed = true;
    }
}
