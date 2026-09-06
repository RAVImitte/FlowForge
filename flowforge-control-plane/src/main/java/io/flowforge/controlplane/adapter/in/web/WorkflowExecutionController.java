package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.execution.WorkflowExecutionService;
import io.flowforge.controlplane.config.TenantContextFilter;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class WorkflowExecutionController {
    private final WorkflowExecutionService service;

    public WorkflowExecutionController(WorkflowExecutionService service) {
        this.service = service;
    }

    @PostMapping("/workflows/{workflowId}/executions")
    ResponseEntity<WorkflowExecutionResponse> start(
            HttpServletRequest request,
            @PathVariable UUID workflowId,
            @RequestHeader(name = "Idempotency-Key", required = false) String idempotencyKey
    ) {
        WorkflowExecutionResponse response = WorkflowExecutionResponse.from(
                service.start(TenantContextFilter.requireTenant(request), workflowId, idempotencyKey)
        );
        return ResponseEntity.accepted()
                .location(URI.create("/api/v1/executions/" + response.id()))
                .body(response);
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
