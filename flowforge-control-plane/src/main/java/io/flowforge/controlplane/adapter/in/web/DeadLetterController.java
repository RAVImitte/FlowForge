package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.recovery.DeadLetterLocation;
import io.flowforge.application.recovery.DeadLetterRecordSummary;
import io.flowforge.application.recovery.DeadLetterReplayReceipt;
import io.flowforge.application.recovery.DeadLetterReplayService;
import io.flowforge.controlplane.config.TenantContextFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/dead-letters/{topic}/partitions/{partition}/offsets/{offset}")
public class DeadLetterController {
    private final DeadLetterReplayService service;

    public DeadLetterController(DeadLetterReplayService service) {
        this.service = service;
    }

    @GetMapping
    DeadLetterRecordSummary inspect(
            HttpServletRequest request,
            @PathVariable String topic,
            @PathVariable int partition,
            @PathVariable long offset
    ) {
        return service.inspect(
                TenantContextFilter.requireTenant(request),
                new DeadLetterLocation(topic, partition, offset)
        );
    }

    @PostMapping("/replay")
    ResponseEntity<DeadLetterReplayReceipt> replay(
            HttpServletRequest request,
            Authentication authentication,
            @PathVariable String topic,
            @PathVariable int partition,
            @PathVariable long offset,
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody ReplayRequest replay
    ) {
        DeadLetterReplayReceipt receipt = service.replay(
                TenantContextFilter.requireTenant(request),
                new DeadLetterLocation(topic, partition, offset),
                parseIdempotencyKey(idempotencyKey),
                authentication == null ? "local-development" : authentication.getName(),
                replay.reason()
        );
        return ResponseEntity.ok(receipt);
    }

    private static UUID parseIdempotencyKey(String value) {
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("Idempotency-Key must be a UUID", invalid);
        }
    }

    public record ReplayRequest(
            @NotBlank
            @Size(min = 10, max = 1000)
            String reason
    ) {
    }
}
