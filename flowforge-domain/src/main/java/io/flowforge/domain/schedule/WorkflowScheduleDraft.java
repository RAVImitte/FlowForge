package io.flowforge.domain.schedule;

import java.util.Objects;
import java.util.UUID;

public record WorkflowScheduleDraft(
        UUID workflowId,
        ScheduleSpec spec,
        MisfirePolicy misfirePolicy
) {
    public WorkflowScheduleDraft {
        Objects.requireNonNull(workflowId, "workflowId must not be null");
        Objects.requireNonNull(spec, "spec must not be null");
        misfirePolicy = misfirePolicy == null ? MisfirePolicy.FIRE_ONCE : misfirePolicy;
    }
}
