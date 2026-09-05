package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.observability.CorrelationIds;
import io.flowforge.observability.LogFields;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class CorrelationIdFilterTest {
    @AfterEach
    void clearContext() {
        MDC.clear();
    }

    @Test
    void returnsTheCallerCorrelationIdAndScopesRequestFields() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/workflows");
        request.addHeader(CorrelationIdFilter.HEADER_NAME, "checkout-request-42");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<Map<String, String>> observed = new AtomicReference<>();

        new CorrelationIdFilter().doFilter(request, response, (ignoredRequest, ignoredResponse) ->
                observed.set(MDC.getCopyOfContextMap())
        );

        assertThat(response.getHeader(CorrelationIdFilter.HEADER_NAME)).isEqualTo("checkout-request-42");
        assertThat(observed.get())
                .containsEntry(LogFields.CORRELATION_ID, "checkout-request-42")
                .containsEntry(LogFields.HTTP_METHOD, "POST")
                .containsEntry(LogFields.HTTP_PATH, "/api/v1/workflows");
        assertThat(MDC.getCopyOfContextMap()).isNull();
    }

    @Test
    void replacesAnUnsafeIdentifierBeforeItReachesLogsOrTheResponse() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/actuator/health");
        request.addHeader(CorrelationIdFilter.HEADER_NAME, "unsafe\r\nforged=value");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<String> observed = new AtomicReference<>();

        new CorrelationIdFilter().doFilter(request, response, (ignoredRequest, ignoredResponse) ->
                observed.set(MDC.get(LogFields.CORRELATION_ID))
        );

        String generated = response.getHeader(CorrelationIdFilter.HEADER_NAME);
        assertThat(CorrelationIds.isSafe(generated)).isTrue();
        assertThat(observed.get()).isEqualTo(generated);
        assertThat(generated).doesNotContain("forged");
        assertThat(MDC.getCopyOfContextMap()).isNull();
    }
}
