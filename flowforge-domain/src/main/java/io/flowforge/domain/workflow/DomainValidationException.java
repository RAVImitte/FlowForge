package io.flowforge.domain.workflow;

import java.util.List;

public final class DomainValidationException extends RuntimeException {
    private final List<String> violations;

    public DomainValidationException(List<String> violations) {
        super("Workflow definition is invalid");
        this.violations = List.copyOf(violations);
    }

    public List<String> violations() {
        return violations;
    }
}
