package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.observability.CorrelationIds;
import io.flowforge.observability.LogContext;
import io.flowforge.observability.LogFields;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter extends OncePerRequestFilter {
    public static final String HEADER_NAME = "X-Correlation-Id";

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {
        String correlationId = CorrelationIds.acceptOrGenerate(request.getHeader(HEADER_NAME));
        response.setHeader(HEADER_NAME, correlationId);
        try (LogContext ignored = LogContext.open(
                LogFields.CORRELATION_ID, correlationId,
                LogFields.HTTP_METHOD, request.getMethod(),
                LogFields.HTTP_PATH, request.getRequestURI()
        )) {
            filterChain.doFilter(request, response);
        }
    }
}
