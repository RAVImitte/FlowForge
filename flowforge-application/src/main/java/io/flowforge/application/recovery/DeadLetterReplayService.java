package io.flowforge.application.recovery;

import io.flowforge.domain.tenancy.TenantId;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;

public class DeadLetterReplayService {
    private static final String EVENT_ID = "flowforge-event-id";
    private static final String EVENT_TYPE = "flowforge-event-type";
    private static final String SCHEMA_VERSION = "flowforge-schema-version";
    private static final String CORRELATION_ID = "flowforge-correlation-id";

    private final DeadLetterTransport transport;
    private final DeadLetterReplayRepository repository;
    private final Clock clock;
    private final Duration transportTimeout;
    private final Duration claimLease;

    public DeadLetterReplayService(
            DeadLetterTransport transport,
            DeadLetterReplayRepository repository,
            Clock clock,
            Duration transportTimeout,
            Duration claimLease
    ) {
        this.transport = transport;
        this.repository = repository;
        this.clock = clock;
        this.transportTimeout = requirePositive(transportTimeout, "transportTimeout");
        this.claimLease = requirePositive(claimLease, "claimLease");
    }

    public DeadLetterRecordSummary inspect(TenantId tenantId, DeadLetterLocation location) {
        return summarize(requireOwnedRecord(tenantId, location));
    }

    public DeadLetterReplayReceipt replay(
            TenantId tenantId,
            DeadLetterLocation location,
            UUID idempotencyKey,
            String actor,
            String reason
    ) {
        if (idempotencyKey == null) throw new IllegalArgumentException("Idempotency-Key must be a UUID");
        actor = requireText(actor, "actor", 200);
        reason = requireText(reason, "reason", 1000);
        if (reason.length() < 10) throw new IllegalArgumentException("reason must contain at least 10 characters");

        DeadLetterRecord record = requireOwnedRecord(tenantId, location);
        Instant now = clock.instant();
        DeadLetterReplayClaim claim = repository.claim(
                tenantId, idempotencyKey, record, actor, reason, now, claimLease
        );
        if (claim.state() == DeadLetterReplayClaim.State.ALREADY_PUBLISHED) {
            return new DeadLetterReplayReceipt(
                    claim.idempotencyKey(), record.recordId(), record.sourceTopic(),
                    DeadLetterReplayReceipt.Status.ALREADY_PUBLISHED, claim.publishedAt()
            );
        }
        if (claim.state() == DeadLetterReplayClaim.State.IN_PROGRESS) {
            throw new DeadLetterReplayConflictException("Dead-letter replay is already in progress");
        }

        try {
            transport.replay(record, transportTimeout);
            Instant publishedAt = clock.instant();
            repository.markPublished(tenantId, claim.idempotencyKey(), publishedAt);
            return new DeadLetterReplayReceipt(
                    claim.idempotencyKey(), record.recordId(), record.sourceTopic(),
                    DeadLetterReplayReceipt.Status.PUBLISHED, publishedAt
            );
        } catch (RuntimeException failure) {
            try {
                repository.markFailed(
                        tenantId, claim.idempotencyKey(), rootCause(failure).getClass().getName(), clock.instant()
                );
            } catch (RuntimeException ledgerFailure) {
                failure.addSuppressed(ledgerFailure);
            }
            throw new DeadLetterReplayUnavailableException(failure);
        }
    }

    private DeadLetterRecord requireOwnedRecord(TenantId tenantId, DeadLetterLocation location) {
        DeadLetterRecord record;
        try {
            record = transport.read(location, transportTimeout)
                    .orElseThrow(DeadLetterRecordNotFoundException::new);
        } catch (DeadLetterRecordNotFoundException | IllegalArgumentException expected) {
            throw expected;
        } catch (RuntimeException unavailable) {
            throw new DeadLetterInspectionUnavailableException(unavailable);
        }
        if (!record.tenantId().equals(tenantId)) throw new DeadLetterRecordNotFoundException();
        return record;
    }

    private static DeadLetterRecordSummary summarize(DeadLetterRecord record) {
        byte[] payload = record.value().getBytes(StandardCharsets.UTF_8);
        return new DeadLetterRecordSummary(
                record.location().topic(), record.location().partition(), record.location().offset(),
                record.sourceTopic(), record.recordId(), record.tenantId().value(), record.key(),
                record.replayHeaders().get(EVENT_ID), record.replayHeaders().get(CORRELATION_ID),
                record.replayHeaders().get(EVENT_TYPE), record.replayHeaders().get(SCHEMA_VERSION),
                record.failureClass(), record.failedAt(), payload.length, sha256(payload)
        );
    }

    private static String sha256(byte[] payload) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static Duration requirePositive(Duration value, String name) {
        if (value == null || value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static String requireText(String value, String name, int maximumLength) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
        value = value.strip();
        if (value.length() > maximumLength) throw new IllegalArgumentException(name + " is too long");
        return value;
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null) current = current.getCause();
        return current;
    }
}
