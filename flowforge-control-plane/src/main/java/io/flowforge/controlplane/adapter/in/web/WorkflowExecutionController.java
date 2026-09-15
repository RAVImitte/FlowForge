package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.execution.WorkflowExecutionService;
import io.flowforge.controlplane.config.TenantContextFilter;
import io.flowforge.domain.execution.WorkflowRunStatus;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.UUID;


@RestController
@RequestMapping("/api/v1")
public class WorkflowExecutionController {
    private final WorkflowExecutionService service;
    private final WorkflowStartAdmissionGate admissionGate;

    public WorkflowExecutionController(
            WorkflowExecutionService service,
            WorkflowStartAdmissionGate admissionGate
    ) {
        this.service = service;
        this.admissionGate = admissionGate;
    }

    @PostMapping("/workflows/{workflowId}/executions")
    ResponseEntity<WorkflowExecutionResponse> start(
            HttpServletRequest request,
            @PathVariable UUID workflowId,
            @RequestHeader(name = "Idempotency-Key", required = false) String idempotencyKey
    ) {
        try (WorkflowStartAdmissionGate.Lease ignored = admissionGate.acquire()) {
            WorkflowExecutionResponse response = WorkflowExecutionResponse.from(
                    service.start(TenantContextFilter.requireTenant(request), workflowId, idempotencyKey)
            );
            return ResponseEntity.accepted()
                    .location(URI.create("/api/v1/executions/" + response.id()))
                    .body(response);
        }
    }

    @GetMapping("/executions")
    ExecutionPageResponse list(
            HttpServletRequest request,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String status
    ) {
        WorkflowRunStatus statusFilter = null;
        if (status != null && !status.isBlank() && !"ALL".equalsIgnoreCase(status)) {
            statusFilter = WorkflowRunStatus.valueOf(status.toUpperCase());
        }
        return ExecutionPageResponse.from(
                service.list(TenantContextFilter.requireTenant(request), page, size, statusFilter)
        );
    }

    @GetMapping("/executions/{executionId}")
    WorkflowExecutionResponse get(HttpServletRequest request, @PathVariable UUID executionId) {
        return WorkflowExecutionResponse.from(
                service.get(TenantContextFilter.requireTenant(request), executionId)
        );
    }


    @PostMapping("/executions/{executionId}/cancel")
    WorkflowExecutionResponse cancel(HttpServletRequest request, @PathVariable UUID executionId) {
        return WorkflowExecutionResponse.from(
                service.cancel(TenantContextFilter.requireTenant(request), executionId)
        );
    }
}
