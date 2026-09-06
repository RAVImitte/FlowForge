package io.flowforge.application.recovery;

public class DeadLetterRecordNotFoundException extends RuntimeException {
    public DeadLetterRecordNotFoundException() {
        super("Dead-letter record was not found");
    }
}
