package io.flowforge.application.coordination;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoordinationPermitServiceTest {
    private static final Instant NOW = Instant.parse("2026-09-05T12:00:00Z");
    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final Duration TTL_PADDING = Duration.ofMinutes(2);

    private CoordinationPermitLedger ledger;
    private EphemeralPermitStore store;
    private CoordinationObserver observer;
    private CoordinationPermitService service;

    @BeforeEach
    void setUp() {
        ledger = mock(CoordinationPermitLedger.class);
        store = mock(EphemeralPermitStore.class);
        observer = mock(CoordinationObserver.class);
        service = new CoordinationPermitService(
                ledger,
                store,
                observer,
                Clock.fixed(NOW, ZoneOffset.UTC),
                LEASE,
                TTL_PADDING
        );
    }

    @Test
    void returnsTheDurablePermitWhenRedisIsUnavailable() {
        CoordinationPermit permit = permit("workflow/one", "worker-a");
        when(ledger.tryAcquire(
                eq(permit.resourceKey()), eq(permit.holderId()), any(), eq(2), eq(NOW), eq(LEASE)
        )).thenReturn(Optional.of(permit));
        when(store.tryAcquire(permit, 2, NOW, TTL_PADDING))
                .thenThrow(new IllegalStateException("redis unavailable"));

        Optional<CoordinationPermit> acquired = service.tryAcquire(
                permit.resourceKey(), permit.holderId(), 2
        );

        assertThat(acquired).contains(permit);
        verify(observer).degraded("acquire");
    }

    @Test
    void reconstructsRedisWhenItsViewRejectsADurablePermit() {
        CoordinationPermit permit = permit("workflow/two", "worker-a");
        when(ledger.tryAcquire(
                eq(permit.resourceKey()), eq(permit.holderId()), any(), eq(2), eq(NOW), eq(LEASE)
        )).thenReturn(Optional.of(permit));
        when(store.tryAcquire(permit, 2, NOW, TTL_PADDING)).thenReturn(false);
        when(ledger.findActive(permit.resourceKey(), NOW)).thenReturn(List.of(permit));

        assertThat(service.tryAcquire(permit.resourceKey(), permit.holderId(), 2)).contains(permit);

        verify(store).replace(permit.resourceKey(), List.of(permit), NOW, TTL_PADDING);
        verify(observer).reconciled(1);
    }

    @Test
    void renewsAndReleasesThroughTheDurableLedgerFirst() {
        CoordinationPermit original = permit("workflow/three", "worker-a");
        CoordinationPermit renewed = new CoordinationPermit(
                original.resourceKey(), original.holderId(), original.token(), NOW.plusSeconds(60)
        );
        when(ledger.renew(original.token(), NOW, LEASE)).thenReturn(Optional.of(renewed));
        when(store.renew(renewed, NOW, TTL_PADDING)).thenReturn(true);
        when(ledger.release(original.token(), NOW)).thenReturn(Optional.of(original));
        when(store.release(original.resourceKey(), original.token(), NOW, TTL_PADDING)).thenReturn(true);

        assertThat(service.renew(original.token())).contains(renewed);
        assertThat(service.release(original)).isTrue();

        verify(ledger).renew(original.token(), NOW, LEASE);
        verify(ledger).release(original.token(), NOW);
    }

    private static CoordinationPermit permit(String resourceKey, String holderId) {
        return new CoordinationPermit(resourceKey, holderId, UUID.randomUUID(), NOW.plus(LEASE));
    }
}
