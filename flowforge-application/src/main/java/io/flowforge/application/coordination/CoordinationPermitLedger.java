package io.flowforge.application.coordination;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface CoordinationPermitLedger {
    Optional<CoordinationPermit> tryAcquire(
            String resourceKey,
            String holderId,
            UUID proposedToken,
            int limit,
            Instant now,
            Duration leaseDuration
    );

    Optional<CoordinationPermit> renew(UUID token, Instant now, Duration leaseDuration);

    Optional<CoordinationPermit> release(UUID token, Instant now);

    List<CoordinationPermit> findActive(String resourceKey, Instant now);
}
