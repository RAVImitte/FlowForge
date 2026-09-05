package io.flowforge.application.coordination;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface EphemeralPermitStore {
    boolean tryAcquire(
            CoordinationPermit permit,
            int limit,
            Instant now,
            Duration ttlPadding
    );

    boolean renew(CoordinationPermit permit, Instant now, Duration ttlPadding);

    boolean release(String resourceKey, UUID token, Instant now, Duration ttlPadding);

    void replace(
            String resourceKey,
            List<CoordinationPermit> permits,
            Instant now,
            Duration ttlPadding
    );

    long activeCount(String resourceKey, Instant now);
}
