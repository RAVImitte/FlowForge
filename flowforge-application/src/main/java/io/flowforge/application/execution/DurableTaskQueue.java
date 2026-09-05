package io.flowforge.application.execution;

import java.time.Instant;
import java.util.UUID;

public interface DurableTaskQueue {
    default ReadyQueueSnapshot readyQueue(Instant now, int requestedLimit) {
        return ReadyQueueSnapshot.unknown(requestedLimit);
    }

    int enqueueReadyTasks(int limit, Instant now);

    int enqueueReadyTasks(UUID workflowExecutionId, int limit, Instant now);

    int releaseDueRetries(int limit, Instant now);
}
