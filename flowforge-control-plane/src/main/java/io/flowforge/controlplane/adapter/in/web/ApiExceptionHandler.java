package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.execution.ExecutionConflictException;
import io.flowforge.application.execution.AdmissionOverloadedException;
import io.flowforge.application.execution.ConcurrencyLimitExceededException;
import io.flowforge.application.execution.ExecutionNotFoundException;
import io.flowforge.application.execution.WorkflowNotPublishedException;
import io.flowforge.application.recovery.DeadLetterRecordNotFoundException;
import io.flowforge.application.recovery.DeadLetterInspectionUnavailableException;
import io.flowforge.application.recovery.DeadLetterReplayConflictException;
import io.flowforge.application.recovery.DeadLetterReplayUnavailableException;
import io.flowforge.application.schedule.ScheduleConflictException;
import io.flowforge.application.schedule.ScheduleNotFoundException;
import io.flowforge.application.tenancy.TenantQuotaConflictException;
import io.flowforge.application.tenancy.TenantQuotaExceededException;
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
    @ExceptionHandler(DeadLetterRecordNotFoundException.class)
    ResponseEntity<ProblemDetail> deadLetterNotFound(
            DeadLetterRecordNotFoundException exception,
            HttpServletRequest request
    ) {
        return problem(HttpStatus.NOT_FOUND, "DEAD_LETTER_NOT_FOUND", exception.getMessage(), request);
    }

    @ExceptionHandler(DeadLetterInspectionUnavailableException.class)
    ResponseEntity<ProblemDetail> deadLetterInspectionUnavailable(
            DeadLetterInspectionUnavailableException exception,
            HttpServletRequest request
    ) {
        return problem(HttpStatus.SERVICE_UNAVAILABLE, "DEAD_LETTER_INSPECTION_UNAVAILABLE",
                exception.getMessage(), request);
    }

    @ExceptionHandler(DeadLetterReplayConflictException.class)
    ResponseEntity<ProblemDetail> deadLetterConflict(
            DeadLetterReplayConflictException exception,
            HttpServletRequest request
    ) {
        return problem(HttpStatus.CONFLICT, "DEAD_LETTER_REPLAY_CONFLICT", exception.getMessage(), request);
    }

    @ExceptionHandler(DeadLetterReplayUnavailableException.class)
    ResponseEntity<ProblemDetail> deadLetterUnavailable(
            DeadLetterReplayUnavailableException exception,
            HttpServletRequest request
    ) {
        return problem(HttpStatus.SERVICE_UNAVAILABLE, "DEAD_LETTER_REPLAY_UNAVAILABLE", exception.getMessage(), request);
    }

    @ExceptionHandler(TenantQuotaExceededException.class)
    ResponseEntity<ProblemDetail> tenantQuotaExceeded(
            TenantQuotaExceededException exception,
            HttpServletRequest request
    ) {
        ProblemDetail detail = detail(
                HttpStatus.TOO_MANY_REQUESTS,
                "TENANT_QUOTA_EXCEEDED",
                exception.getMessage(),
                request
        );
        detail.setProperty("tenantId", exception.tenantId().value());
        detail.setProperty("quota", exception.quota());
        detail.setProperty("limit", exception.limit());
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", "1")
                .body(detail);
    }

    @ExceptionHandler(TenantQuotaConflictException.class)
    ResponseEntity<ProblemDetail> tenantQuotaConflict(
            TenantQuotaConflictException exception,
            HttpServletRequest request
    ) {
        return problem(HttpStatus.CONFLICT, "TENANT_QUOTA_CONFLICT", exception.getMessage(), request);
    }

    @ExceptionHandler(ConcurrencyLimitExceededException.class)
    ResponseEntity<ProblemDetail> concurrencyLimitExceeded(
            ConcurrencyLimitExceededException exception,
            HttpServletRequest request
    ) {
        ProblemDetail detail = detail(
                HttpStatus.TOO_MANY_REQUESTS,
                "WORKFLOW_CONCURRENCY_LIMIT_EXCEEDED",
                exception.getMessage(),
                request
        );
        detail.setProperty("workflowId", exception.workflowId());
        detail.setProperty("limit", exception.limit());
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", "1")
                .body(detail);
    }

    @ExceptionHandler(AdmissionOverloadedException.class)
    ResponseEntity<ProblemDetail> admissionOverloaded(
            AdmissionOverloadedException exception,
            HttpServletRequest request
    ) {
        ProblemDetail detail = detail(
                HttpStatus.TOO_MANY_REQUESTS,
                "WORKFLOW_READY_QUEUE_SATURATED",
                exception.getMessage(),
                request
        );
        detail.setProperty("workflowId", exception.workflowId());
        detail.setProperty("limit", exception.limit());
        long retryAfterSeconds = Math.max(1, (exception.retryAfter().toMillis() + 999) / 1_000);
        detail.setProperty("retryAfterSeconds", retryAfterSeconds);
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", Long.toString(retryAfterSeconds))
                .body(detail);
    }

    @ExceptionHandler(ScheduleNotFoundException.class)
    ResponseEntity<ProblemDetail> scheduleNotFound(
            ScheduleNotFoundException exception,
            HttpServletRequest request
    ) {
        return problem(HttpStatus.NOT_FOUND, "SCHEDULE_NOT_FOUND", exception.getMessage(), request);
    }

    @ExceptionHandler(ScheduleConflictException.class)
    ResponseEntity<ProblemDetail> scheduleConflict(
            ScheduleConflictException exception,
            HttpServletRequest request
    ) {
        return problem(HttpStatus.CONFLICT, "SCHEDULE_CONFLICT", exception.getMessage(), request);
    }

    @ExceptionHandler(ExecutionNotFoundException.class)
    ResponseEntity<ProblemDetail> executionNotFound(
            ExecutionNotFoundException exception,
            HttpServletRequest request
    ) {
        return problem(HttpStatus.NOT_FOUND, "EXECUTION_NOT_FOUND", exception.getMessage(), request);
    }

    @ExceptionHandler(WorkflowNotPublishedException.class)
    ResponseEntity<ProblemDetail> workflowNotPublished(
            WorkflowNotPublishedException exception,
            HttpServletRequest request
    ) {
        return problem(HttpStatus.CONFLICT, "WORKFLOW_NOT_PUBLISHED", exception.getMessage(), request);
    }

    @ExceptionHandler(ExecutionConflictException.class)
    ResponseEntity<ProblemDetail> executionConflict(
            ExecutionConflictException exception,
            HttpServletRequest request
    ) {
        return problem(HttpStatus.CONFLICT, "EXECUTION_CONFLICT", exception.getMessage(), request);
    }

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
