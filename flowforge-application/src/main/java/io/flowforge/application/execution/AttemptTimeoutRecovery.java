package io.flowforge.application.execution;

import java.time.Instant;

public interface AttemptTimeoutRecovery {
    int reapTimedOutAttempts(int limit, Instant now);
}
