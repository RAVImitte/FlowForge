package io.flowforge.worker.persistence;

import java.util.UUID;

public record CommandReceipt(boolean newlyReceived, boolean completed, UUID resultEventId) {
}
