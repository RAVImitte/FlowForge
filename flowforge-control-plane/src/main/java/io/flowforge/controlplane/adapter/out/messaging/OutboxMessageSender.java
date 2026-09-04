package io.flowforge.controlplane.adapter.out.messaging;

import java.time.Duration;

public interface OutboxMessageSender {
    void send(OutboxMessage message, Duration timeout) throws Exception;
}
