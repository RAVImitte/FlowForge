package io.flowforge.application.execution;

import java.time.Instant;

public interface ConcurrencyPermitRecovery {
    int reconcileConcurrencyPermits(int limit, Instant now);
}
