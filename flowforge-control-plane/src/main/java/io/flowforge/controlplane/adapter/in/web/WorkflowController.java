package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.controlplane.config.TenantContextFilter;
import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.TaskDependency;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/workflows")
public class WorkflowController {
    private final WorkflowService service;

    public WorkflowController(WorkflowService service) {
        this.service = service;
    }

    @PostMapping
    ResponseEntity<WorkflowResponse> create(
            HttpServletRequest httpRequest,
            @Valid @RequestBody WorkflowRequest request
    ) {
        TenantId tenantId = TenantContextFilter.requireTenant(httpRequest);
        WorkflowDefinition created = service.create(tenantId, toDraft(request));
        return ResponseEntity
                .created(URI.create("/api/v1/workflows/" + created.id()))
                .eTag(etag(created.lockVersion()))
                .body(WorkflowResponse.from(created));
    }

    @GetMapping("/{id}")
    ResponseEntity<WorkflowResponse> get(HttpServletRequest request, @PathVariable UUID id) {
        WorkflowDefinition workflow = service.get(TenantContextFilter.requireTenant(request), id);
        return ResponseEntity.ok()
                .eTag(etag(workflow.lockVersion()))
                .body(WorkflowResponse.from(workflow));
    }

    @GetMapping
    WorkflowPageResponse list(
            HttpServletRequest request,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return WorkflowPageResponse.from(
                service.list(TenantContextFilter.requireTenant(request), page, size)
        );
    }

    @PutMapping("/{id}")
    ResponseEntity<WorkflowResponse> update(
            HttpServletRequest httpRequest,
            @PathVariable UUID id,
            @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch,
            @Valid @RequestBody WorkflowRequest request
    ) {
        WorkflowDefinition updated = service.update(
                TenantContextFilter.requireTenant(httpRequest),
                id,
                parseEtag(ifMatch),
                toDraft(request)
        );
        return ResponseEntity.ok()
                .eTag(etag(updated.lockVersion()))
                .body(WorkflowResponse.from(updated));
    }

    @PostMapping("/{id}/publish")
    ResponseEntity<WorkflowResponse> publish(
            HttpServletRequest request,
            @PathVariable UUID id,
            @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch
    ) {
        WorkflowDefinition published = service.publish(
                TenantContextFilter.requireTenant(request), id, parseEtag(ifMatch)
        );
        return ResponseEntity.ok()
                .eTag(etag(published.lockVersion()))
                .body(WorkflowResponse.from(published));
    }

    @DeleteMapping("/{id}")
    ResponseEntity<Void> archive(
            HttpServletRequest request,
            @PathVariable UUID id,
            @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch
    ) {
        service.archive(TenantContextFilter.requireTenant(request), id, parseEtag(ifMatch));
        return ResponseEntity.noContent().build();
    }

    private static WorkflowDraft toDraft(WorkflowRequest request) {
        List<TaskDefinition> tasks = request.tasks().stream()
                .map(task -> new TaskDefinition(
                        task.key(),
                        task.name(),
                        task.type(),
                        task.configuration(),
                        task.secretReferences() == null
                                ? java.util.Map.of()
                                : task.secretReferences().entrySet().stream().collect(
                                        java.util.stream.Collectors.toUnmodifiableMap(
                                                java.util.Map.Entry::getKey,
                                                entry -> entry.getValue().toDomain()
                                        )
                                ),
                        task.reliabilityPolicy() == null
                                ? null
                                : task.reliabilityPolicy().toDomain(),
                        task.maxConcurrency()
                ))
                .toList();
        List<TaskDependency> dependencies = request.dependencies() == null
                ? List.of()
                : request.dependencies().stream()
                        .map(dependency -> new TaskDependency(
                                dependency.taskKey(), dependency.dependsOnTaskKey()
                        ))
                        .toList();
        return new WorkflowDraft(
                request.name(), request.description(), tasks, dependencies, request.maxConcurrentExecutions()
        );
    }

    private static long parseEtag(String value) {
        if (value == null) throw new PreconditionRequiredException();
        if (!value.matches("\"[0-9]+\"")) {
            throw new IllegalArgumentException("If-Match must contain one strong numeric ETag");
        }
        try {
            return Long.parseLong(value.substring(1, value.length() - 1));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("If-Match ETag is outside the supported range", exception);
        }
    }

    private static String etag(long lockVersion) {
        return "\"" + lockVersion + "\"";
    }
}
