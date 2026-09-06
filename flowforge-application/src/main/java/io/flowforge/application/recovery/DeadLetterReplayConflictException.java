package io.flowforge.application.recovery;

public class DeadLetterReplayConflictException extends RuntimeException {
    public DeadLetterReplayConflictException(String message) {
        super(message);
    }
}
