package io.flowforge.application.recovery;

public class DeadLetterInspectionUnavailableException extends RuntimeException {
    public DeadLetterInspectionUnavailableException(Throwable cause) {
        super("Dead-letter record could not be inspected", cause);
    }
}
