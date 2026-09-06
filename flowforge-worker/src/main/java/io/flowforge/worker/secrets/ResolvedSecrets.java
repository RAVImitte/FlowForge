package io.flowforge.worker.secrets;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ResolvedSecrets implements AutoCloseable {
    private final Map<String, String> values;

    ResolvedSecrets(Map<String, String> values) {
        this.values = new LinkedHashMap<>(values);
    }

    public Map<String, String> values() {
        return Map.copyOf(values);
    }

    public List<String> valuesForRedaction() {
        return List.copyOf(values.values());
    }

    @Override
    public void close() {
        values.clear();
    }

    @Override
    public String toString() {
        return "ResolvedSecrets[bindings=" + values.keySet() + "]";
    }
}
