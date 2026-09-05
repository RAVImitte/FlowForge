package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.schedule.WorkflowScheduleService;
import io.flowforge.domain.schedule.WorkflowSchedule;
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
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/schedules")
public class ScheduleController {
    private final WorkflowScheduleService service;

    public ScheduleController(WorkflowScheduleService service) {
        this.service = service;
    }

    @PostMapping
    ResponseEntity<ScheduleResponse> create(@Valid @RequestBody ScheduleRequest request) {
        WorkflowSchedule created = service.create(request.toDraft());
        return ResponseEntity.created(URI.create("/api/v1/schedules/" + created.id()))
                .eTag(etag(created.lockVersion()))
                .body(ScheduleResponse.from(created));
    }

    @GetMapping("/{id}")
    ResponseEntity<ScheduleResponse> get(@PathVariable UUID id) {
        WorkflowSchedule schedule = service.get(id);
        return ResponseEntity.ok()
                .eTag(etag(schedule.lockVersion()))
                .body(ScheduleResponse.from(schedule));
    }

    @GetMapping
    SchedulePageResponse list(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return SchedulePageResponse.from(service.list(page, size));
    }

    @PutMapping("/{id}")
    ResponseEntity<ScheduleResponse> update(
            @PathVariable UUID id,
            @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch,
            @Valid @RequestBody ScheduleRequest request
    ) {
        WorkflowSchedule updated = service.update(id, parseEtag(ifMatch), request.toDraft());
        return ResponseEntity.ok().eTag(etag(updated.lockVersion())).body(ScheduleResponse.from(updated));
    }

    @PostMapping("/{id}/pause")
    ResponseEntity<ScheduleResponse> pause(
            @PathVariable UUID id,
            @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch
    ) {
        WorkflowSchedule paused = service.pause(id, parseEtag(ifMatch));
        return ResponseEntity.ok().eTag(etag(paused.lockVersion())).body(ScheduleResponse.from(paused));
    }

    @PostMapping("/{id}/resume")
    ResponseEntity<ScheduleResponse> resume(
            @PathVariable UUID id,
            @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch
    ) {
        WorkflowSchedule resumed = service.resume(id, parseEtag(ifMatch));
        return ResponseEntity.ok().eTag(etag(resumed.lockVersion())).body(ScheduleResponse.from(resumed));
    }

    @DeleteMapping("/{id}")
    ResponseEntity<Void> delete(
            @PathVariable UUID id,
            @RequestHeader(value = HttpHeaders.IF_MATCH, required = false) String ifMatch
    ) {
        service.delete(id, parseEtag(ifMatch));
        return ResponseEntity.noContent().build();
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
