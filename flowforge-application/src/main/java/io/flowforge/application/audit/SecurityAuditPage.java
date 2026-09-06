package io.flowforge.application.audit;

import java.util.List;

public record SecurityAuditPage(List<SecurityAuditEvent> items, int page, int size, long totalElements) {
    public SecurityAuditPage {
        items = List.copyOf(items);
    }
}
