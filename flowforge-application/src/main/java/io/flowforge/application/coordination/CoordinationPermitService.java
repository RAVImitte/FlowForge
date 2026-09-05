package io.flowforge.application.coordination;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class CoordinationPermitService {
    private final CoordinationPermitLedger ledger;
    private final EphemeralPermitStore ephemeralStore;
    private final CoordinationObserver observer;
    private final Clock clock;
    private final Duration leaseDuration;
    private final Duration ttlPadding;

    public CoordinationPermitService(
            CoordinationPermitLedger ledger,
            EphemeralPermitStore ephemeralStore,
            CoordinationObserver observer,
            Clock clock,
            Duration leaseDuration,
            Duration ttlPadding
    ) {
        this.ledger = Objects.requireNonNull(ledger);
        this.ephemeralStore = Objects.requireNonNull(ephemeralStore);
        this.observer = Objects.requireNonNull(observer);
        this.clock = Objects.requireNonNull(clock);
        this.leaseDuration = positiveBounded(leaseDuration, "leaseDuration");
        this.ttlPadding = positiveBounded(ttlPadding, "ttlPadding");
    }

    public Optional<CoordinationPermit> tryAcquire(String resourceKey, String holderId, int limit) {
        if (limit < 1 || limit > 100_000) {
            throw new IllegalArgumentException("limit must be between 1 and 100000");
        }
        Instant now = clock.instant();
        Optional<CoordinationPermit> acquired = ledger.tryAcquire(
                resourceKey, holderId, UUID.randomUUID(), limit, now, leaseDuration
        );
        acquired.ifPresent(permit -> mirrorAcquire(permit, limit, now));
        return acquired;
    }

    public Optional<CoordinationPermit> renew(UUID token) {
        Objects.requireNonNull(token, "token must not be null");
        Instant now = clock.instant();
        Optional<CoordinationPermit> renewed = ledger.renew(token, now, leaseDuration);
        renewed.ifPresent(permit -> {
            try {
                if (!ephemeralStore.renew(permit, now, ttlPadding)) {
                    reconcile(permit.resourceKey());
                }
            } catch (RuntimeException unavailable) {
                observer.degraded("renew");
            }
        });
        return renewed;
    }

    public boolean release(CoordinationPermit permit) {
        Objects.requireNonNull(permit, "permit must not be null");
        Instant now = clock.instant();
        Optional<CoordinationPermit> released = ledger.release(permit.token(), now);
        if (released.isEmpty()) return false;
        CoordinationPermit authoritative = released.get();
        try {
            if (!ephemeralStore.release(
                    authoritative.resourceKey(), authoritative.token(), now, ttlPadding
            )) {
                reconcile(authoritative.resourceKey());
            }
        } catch (RuntimeException unavailable) {
            observer.degraded("release");
        }
        return true;
    }

    public int reconcile(String resourceKey) {
        Instant now = clock.instant();
        List<CoordinationPermit> active = ledger.findActive(resourceKey, now);
        try {
            ephemeralStore.replace(resourceKey, active, now, ttlPadding);
            observer.reconciled(active.size());
        } catch (RuntimeException unavailable) {
            observer.degraded("reconcile");
        }
        return active.size();
    }

    private void mirrorAcquire(CoordinationPermit permit, int limit, Instant now) {
        try {
            if (!ephemeralStore.tryAcquire(permit, limit, now, ttlPadding)) {
                reconcile(permit.resourceKey());
            }
        } catch (RuntimeException unavailable) {
            observer.degraded("acquire");
        }
    }

    private static Duration positiveBounded(Duration value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isZero() || value.isNegative() || value.compareTo(Duration.ofDays(1)) > 0) {
            throw new IllegalArgumentException(name + " must be positive and at most one day");
        }
        return value;
    }
}
