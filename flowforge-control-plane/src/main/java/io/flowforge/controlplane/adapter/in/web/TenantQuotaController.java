package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.tenancy.TenantQuotaService;
import io.flowforge.controlplane.config.TenantContextFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/tenant/quota")
public class TenantQuotaController {
    private final TenantQuotaService service;

    public TenantQuotaController(TenantQuotaService service) {
        this.service = service;
    }

    @GetMapping
    ResponseEntity<TenantQuotaResponse> get(HttpServletRequest request) {
        TenantQuotaResponse response = TenantQuotaResponse.from(
                service.get(TenantContextFilter.requireTenant(request))
        );
        return ResponseEntity.ok().eTag(Long.toString(response.version())).body(response);
    }

    @PutMapping
    ResponseEntity<TenantQuotaResponse> update(
            HttpServletRequest request,
            @RequestHeader(name = "If-Match", required = false) String ifMatch,
            @Valid @RequestBody TenantQuotaRequest quota
    ) {
        TenantQuotaResponse response = TenantQuotaResponse.from(service.update(
                TenantContextFilter.requireTenant(request), quota.toPolicy(), expectedVersion(ifMatch)
        ));
        return ResponseEntity.ok().eTag(Long.toString(response.version())).body(response);
    }

    private static long expectedVersion(String ifMatch) {
        if (ifMatch == null || ifMatch.isBlank()) throw new PreconditionRequiredException();
        String value = ifMatch.strip();
        if (value.startsWith("W/")) value = value.substring(2).strip();
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            value = value.substring(1, value.length() - 1);
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException("If-Match must contain a numeric quota version");
        }
    }
}
