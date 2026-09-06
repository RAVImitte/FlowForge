package io.flowforge.observability;

import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Captures and restores W3C trace propagation fields across durable queue gaps.
 */
public final class TraceContextPropagation {
    private static final String TRACE_PARENT = "traceparent";
    private static final String TRACE_STATE = "tracestate";
    private static final String BAGGAGE = "baggage";
    private static final TextMapPropagator PROPAGATOR = TextMapPropagator.composite(
            W3CTraceContextPropagator.getInstance(),
            W3CBaggagePropagator.getInstance()
    );
    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier.get(key);
        }
    };

    private TraceContextPropagation() {
    }

    public static TraceContextSnapshot capture() {
        Map<String, String> carrier = new LinkedHashMap<>();
        PROPAGATOR.inject(Context.current(), carrier, Map::put);
        return new TraceContextSnapshot(
                carrier.get(TRACE_PARENT),
                carrier.get(TRACE_STATE),
                carrier.get(BAGGAGE)
        );
    }

    public static Scope restore(TraceContextSnapshot snapshot) {
        if (snapshot == null || snapshot.isEmpty()) return Context.current().makeCurrent();
        Map<String, String> carrier = new LinkedHashMap<>();
        put(carrier, TRACE_PARENT, snapshot.traceParent());
        put(carrier, TRACE_STATE, snapshot.traceState());
        put(carrier, BAGGAGE, snapshot.baggage());
        return PROPAGATOR.extract(Context.root(), carrier, GETTER).makeCurrent();
    }

    private static void put(Map<String, String> carrier, String name, String value) {
        if (value != null) carrier.put(name, value);
    }
}
