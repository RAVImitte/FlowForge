package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.workflow.WorkflowConflictException;
import io.flowforge.application.workflow.WorkflowNotFoundException;
import io.flowforge.domain.workflow.DomainValidationException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.util.List;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(WorkflowNotFoundException.class)
    ResponseEntity<ProblemDetail> notFound(WorkflowNotFoundException exception, HttpServletRequest request) {
        return problem(HttpStatus.NOT_FOUND, "WORKFLOW_NOT_FOUND", exception.getMessage(), request);
    }

    @ExceptionHandler(WorkflowConflictException.class)
    ResponseEntity<ProblemDetail> conflict(WorkflowConflictException exception, HttpServletRequest request) {
        return problem(HttpStatus.CONFLICT, "WORKFLOW_CONFLICT", exception.getMessage(), request);
    }

    @ExceptionHandler(PreconditionRequiredException.class)
    ResponseEntity<ProblemDetail> precondition(
            PreconditionRequiredException exception,
            HttpServletRequest request
    ) {
        return problem(HttpStatus.PRECONDITION_REQUIRED, "IF_MATCH_REQUIRED", exception.getMessage(), request);
    }

    @ExceptionHandler(DomainValidationException.class)
    ResponseEntity<ProblemDetail> domainValidation(
            DomainValidationException exception,
            HttpServletRequest request
    ) {
        ProblemDetail detail = detail(
                HttpStatus.UNPROCESSABLE_CONTENT,
                "INVALID_WORKFLOW",
                exception.getMessage(),
                request
        );
        detail.setProperty("violations", exception.violations());
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_CONTENT).body(detail);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ProblemDetail> beanValidation(
            MethodArgumentNotValidException exception,
            HttpServletRequest request
    ) {
        List<String> violations = exception.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .toList();
        ProblemDetail detail = detail(
                HttpStatus.BAD_REQUEST,
                "INVALID_REQUEST",
                "Request validation failed",
                request
        );
        detail.setProperty("violations", violations);
        return ResponseEntity.badRequest().body(detail);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<ProblemDetail> badRequest(IllegalArgumentException exception, HttpServletRequest request) {
        return problem(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", exception.getMessage(), request);
    }

    private static ResponseEntity<ProblemDetail> problem(
            HttpStatus status,
            String code,
            String message,
            HttpServletRequest request
    ) {
        return ResponseEntity.status(status).body(detail(status, code, message, request));
    }

    private static ProblemDetail detail(
            HttpStatus status,
            String code,
            String message,
            HttpServletRequest request
    ) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(status, message);
        detail.setTitle(status.getReasonPhrase());
        detail.setInstance(URI.create(request.getRequestURI()));
        detail.setProperty("code", code);
        return detail;
    }
}
