package io.flowforge.observability;

/**
 * Bounded W3C propagation fields persisted beside durable messages.
 */
public record TraceContextSnapshot(String traceParent, String traceState, String baggage) {
    private static final int MAX_TRACE_PARENT_LENGTH = 512;
    private static final int MAX_TRACE_STATE_LENGTH = 512;
    private static final int MAX_BAGGAGE_LENGTH = 8_192;
    private static final TraceContextSnapshot EMPTY = new TraceContextSnapshot(null, null, null);

    public TraceContextSnapshot {
        traceParent = bounded(traceParent, MAX_TRACE_PARENT_LENGTH, "traceparent");
        traceState = bounded(traceState, MAX_TRACE_STATE_LENGTH, "tracestate");
        baggage = bounded(baggage, MAX_BAGGAGE_LENGTH, "baggage");
    }

    public static TraceContextSnapshot empty() {
        return EMPTY;
    }

    public boolean isEmpty() {
        return traceParent == null && traceState == null && baggage == null;
    }

    private static String bounded(String value, int maximumLength, String field) {
        if (value == null || value.isBlank()) return null;
        if (value.length() > maximumLength) {
            throw new IllegalArgumentException(field + " exceeds " + maximumLength + " characters");
        }
        if (value.chars().anyMatch(character -> character < 0x20 || character == 0x7f)) {
            throw new IllegalArgumentException(field + " contains a control character");
        }
        return value;
    }
}
