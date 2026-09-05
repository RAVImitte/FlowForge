package io.flowforge.application.schedule;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface ScheduleFireRepository {
    ScheduleMaterializationResult materializeDue(int limit, Instant now, Duration misfireThreshold);

    List<ClaimedScheduleFire> claimPending(
            int limit,
            String claimant,
            Instant now,
            Duration leaseDuration
    );

    boolean markStarted(UUID triggerId, UUID claimToken, UUID workflowExecutionId, Instant now);

    boolean markFailed(UUID triggerId, UUID claimToken, String errorMessage, Instant now);

    boolean release(UUID triggerId, UUID claimToken, String errorMessage, Instant availableAt);
}
