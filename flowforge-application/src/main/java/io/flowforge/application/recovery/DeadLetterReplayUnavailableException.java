package io.flowforge.application.recovery;

public class DeadLetterReplayUnavailableException extends RuntimeException {
    public DeadLetterReplayUnavailableException(Throwable cause) {
        super("Dead-letter replay could not be published", cause);
    }
}
