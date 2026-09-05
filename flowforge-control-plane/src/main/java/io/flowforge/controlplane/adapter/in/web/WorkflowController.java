package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.TaskDependency;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
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
    ResponseEntity<WorkflowResponse> create(@Valid @RequestBody WorkflowRequest request) {
        WorkflowDefinition created = service.create(toDraft(request));
        return ResponseEntity
                .created(URI.create("/api/v1/workflows/" + created.id()))
                .eTag(etag(created.lockVersion()))
                .body(WorkflowResponse.from(created));
    }

    @GetMapping("/{id}")
    ResponseEntity<WorkflowResponse> get(@PathVariable UUID id) {
        WorkflowDefinition workflow = service.get(id);
        return ResponseEntity.ok()
                .eTag(etag(workflow.lockVersion()))
                .body(WorkflowResponse.from(workflow));
    }

    @GetMapping
    WorkflowPageResponse list(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return WorkflowPageResponse.from(service.list(page, size));
    }

    @PutMapping("/{id}")
    ResponseEntity<WorkflowResponse> update(
            @PathVariable UUID id,
            @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch,
            @Valid @RequestBody WorkflowRequest request
    ) {
        WorkflowDefinition updated = service.update(id, parseEtag(ifMatch), toDraft(request));
        return ResponseEntity.ok()
                .eTag(etag(updated.lockVersion()))
                .body(WorkflowResponse.from(updated));
    }

    @PostMapping("/{id}/publish")
    ResponseEntity<WorkflowResponse> publish(
            @PathVariable UUID id,
            @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch
    ) {
        WorkflowDefinition published = service.publish(id, parseEtag(ifMatch));
        return ResponseEntity.ok()
                .eTag(etag(published.lockVersion()))
                .body(WorkflowResponse.from(published));
    }

    @DeleteMapping("/{id}")
    ResponseEntity<Void> archive(
            @PathVariable UUID id,
            @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch
    ) {
        service.archive(id, parseEtag(ifMatch));
        return ResponseEntity.noContent().build();
    }

    private static WorkflowDraft toDraft(WorkflowRequest request) {
        List<TaskDefinition> tasks = request.tasks().stream()
                .map(task -> new TaskDefinition(
                        task.key(),
                        task.name(),
                        task.type(),
                        task.configuration(),
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
