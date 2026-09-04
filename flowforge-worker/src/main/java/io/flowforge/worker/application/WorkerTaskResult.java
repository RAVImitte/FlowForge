package io.flowforge.worker.application;

import io.flowforge.messaging.TaskResultOutcomeV1;

import java.util.Objects;

public record WorkerTaskResult(TaskResultOutcomeV1 outcome, String errorCode, String errorMessage) {
    public WorkerTaskResult {
        Objects.requireNonNull(outcome, "outcome must not be null");
        errorCode = normalize(errorCode);
        errorMessage = normalize(errorMessage);
    }

    public static WorkerTaskResult succeeded() {
        return new WorkerTaskResult(TaskResultOutcomeV1.SUCCEEDED, null, null);
    }

    public static WorkerTaskResult failed(String errorCode, String errorMessage) {
        return new WorkerTaskResult(TaskResultOutcomeV1.FAILED, errorCode, errorMessage);
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
