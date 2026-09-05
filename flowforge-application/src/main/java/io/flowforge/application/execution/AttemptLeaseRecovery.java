package io.flowforge.application.execution;

import java.time.Instant;

public interface AttemptLeaseRecovery {
    int reapExpiredLeases(int limit, Instant now);
}
