package io.flowforge.application.recovery;

import io.flowforge.domain.tenancy.TenantId;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeadLetterReplayServiceTest {
    private static final Instant NOW = Instant.parse("2026-09-06T12:00:00Z");
    private static final TenantId TENANT = new TenantId("tenant-a");
    private static final DeadLetterLocation LOCATION = new DeadLetterLocation("results.dlq", 2, 41);

    private final DeadLetterTransport transport = mock(DeadLetterTransport.class);
    private final DeadLetterReplayRepository repository = mock(DeadLetterReplayRepository.class);
    private final DeadLetterReplayService service = new DeadLetterReplayService(
            transport, repository, Clock.fixed(NOW, ZoneOffset.UTC),
            Duration.ofSeconds(5), Duration.ofSeconds(30)
    );

    @Test
    void exposesMetadataAndPayloadFingerprintWithoutReturningThePayload() {
        when(transport.read(LOCATION, Duration.ofSeconds(5))).thenReturn(Optional.of(record()));

        DeadLetterRecordSummary summary = service.inspect(TENANT, LOCATION);

        assertThat(summary.tenantId()).isEqualTo("tenant-a");
        assertThat(summary.sourceTopic()).isEqualTo("results");
        assertThat(summary.payloadBytes()).isEqualTo(17);
        assertThat(summary.payloadSha256()).hasSize(64);
    }

    @Test
    void publishesOneClaimedReplayWithItsStableHeaders() {
        UUID key = UUID.randomUUID();
        DeadLetterRecord record = record();
        when(transport.read(LOCATION, Duration.ofSeconds(5))).thenReturn(Optional.of(record));
        when(repository.claim(TENANT, key, record, "operator", "consumer fix deployed", NOW, Duration.ofSeconds(30)))
                .thenReturn(new DeadLetterReplayClaim(key, DeadLetterReplayClaim.State.ACQUIRED, null));

        DeadLetterReplayReceipt receipt = service.replay(
                TENANT, LOCATION, key, "operator", "consumer fix deployed"
        );

        assertThat(receipt.status()).isEqualTo(DeadLetterReplayReceipt.Status.PUBLISHED);
        verify(transport).replay(record, Duration.ofSeconds(5));
        verify(repository).markPublished(TENANT, key, NOW);
    }

    @Test
    void returnsTheOriginalReceiptWithoutPublishingAnIdempotentDuplicate() {
        UUID key = UUID.randomUUID();
        DeadLetterRecord record = record();
        when(transport.read(LOCATION, Duration.ofSeconds(5))).thenReturn(Optional.of(record));
        when(repository.claim(TENANT, key, record, "operator", "consumer fix deployed", NOW, Duration.ofSeconds(30)))
                .thenReturn(new DeadLetterReplayClaim(key, DeadLetterReplayClaim.State.ALREADY_PUBLISHED, NOW));

        DeadLetterReplayReceipt receipt = service.replay(
                TENANT, LOCATION, key, "operator", "consumer fix deployed"
        );

        assertThat(receipt.status()).isEqualTo(DeadLetterReplayReceipt.Status.ALREADY_PUBLISHED);
        verify(transport, never()).replay(record, Duration.ofSeconds(5));
    }

    @Test
    void hidesRecordsOwnedByAnotherTenant() {
        when(transport.read(LOCATION, Duration.ofSeconds(5))).thenReturn(Optional.of(record()));

        assertThatThrownBy(() -> service.inspect(new TenantId("tenant-b"), LOCATION))
                .isInstanceOf(DeadLetterRecordNotFoundException.class);
    }

    @Test
    void reportsTransportFailureAsInspectionUnavailable() {
        when(transport.read(LOCATION, Duration.ofSeconds(5)))
                .thenThrow(new IllegalStateException("broker unavailable"));

        assertThatThrownBy(() -> service.inspect(TENANT, LOCATION))
                .isInstanceOf(DeadLetterInspectionUnavailableException.class)
                .hasMessage("Dead-letter record could not be inspected");
    }

    private static DeadLetterRecord record() {
        return new DeadLetterRecord(
                LOCATION,
                "results",
                "results:2:17",
                TENANT,
                "execution-1",
                "{\"poison\":\"body\"}",
                Map.of(
                        "flowforge-event-id", UUID.randomUUID().toString(),
                        "flowforge-correlation-id", UUID.randomUUID().toString(),
                        "flowforge-tenant-id", TENANT.value()
                ),
                "example.ContractFailure",
                NOW.minusSeconds(60)
        );
    }
}
