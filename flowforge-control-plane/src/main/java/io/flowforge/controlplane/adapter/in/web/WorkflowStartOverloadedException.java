package io.flowforge.controlplane.adapter.in.web;

import java.time.Duration;

final class WorkflowStartOverloadedException extends RuntimeException {
    private final int limit;
    private final Duration retryAfter;

    WorkflowStartOverloadedException(int limit, Duration retryAfter) {
        super("Workflow-start admission is temporarily saturated");
        this.limit = limit;
        this.retryAfter = retryAfter;
    }

    int limit() {
        return limit;
    }

    Duration retryAfter() {
        return retryAfter;
    }
}
