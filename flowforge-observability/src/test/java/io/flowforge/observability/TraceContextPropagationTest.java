package io.flowforge.observability;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TraceContextPropagationTest {
    @Test
    void capturesAndRestoresTraceStateAndBaggage() {
        SpanContext spanContext = SpanContext.create(
                "0123456789abcdef0123456789abcdef",
                "0123456789abcdef",
                TraceFlags.getSampled(),
                TraceState.builder().put("vendor", "state").build()
        );
        Context source = Baggage.builder().put("tenant", "portfolio").build()
                .storeInContext(Context.root().with(Span.wrap(spanContext)));

        TraceContextSnapshot snapshot;
        try (Scope ignored = source.makeCurrent()) {
            snapshot = TraceContextPropagation.capture();
        }

        assertThat(snapshot.traceParent()).isEqualTo("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01");
        assertThat(snapshot.traceState()).isEqualTo("vendor=state");
        assertThat(snapshot.baggage()).isEqualTo("tenant=portfolio");

        try (Scope ignored = TraceContextPropagation.restore(snapshot)) {
            assertThat(Span.current().getSpanContext().getTraceId()).isEqualTo(spanContext.getTraceId());
            assertThat(Baggage.current().getEntryValue("tenant")).isEqualTo("portfolio");
        }
        assertThat(Span.current().getSpanContext().isValid()).isFalse();
    }

    @Test
    void rejectsUnboundedOrUnsafePropagationValues() {
        assertThatThrownBy(() -> new TraceContextSnapshot("x".repeat(513), null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TraceContextSnapshot(null, null, "unsafe\nvalue"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
