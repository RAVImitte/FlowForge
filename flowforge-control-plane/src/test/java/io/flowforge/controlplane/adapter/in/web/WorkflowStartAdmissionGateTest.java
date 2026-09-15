package io.flowforge.controlplane.adapter.in.web;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowStartAdmissionGateTest {
    @Test
    void rejectsImmediatelyAtCapacityAndRecoversWhenAPermitIsReleased() {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        WorkflowStartAdmissionGate gate = new WorkflowStartAdmissionGate(meters, 1, Duration.ofSeconds(2));

        WorkflowStartAdmissionGate.Lease lease = gate.acquire();

        assertThatThrownBy(gate::acquire)
                .isInstanceOf(WorkflowStartOverloadedException.class)
                .satisfies(error -> {
                    WorkflowStartOverloadedException overloaded = (WorkflowStartOverloadedException) error;
                    assertThat(overloaded.limit()).isEqualTo(1);
                    assertThat(overloaded.retryAfter()).isEqualTo(Duration.ofSeconds(2));
                });
        assertThat(meters.get("flowforge.admission.rejected").counter().count()).isEqualTo(1.0);

        lease.close();
        try (WorkflowStartAdmissionGate.Lease ignored = gate.acquire()) {
            assertThat(meters.get("flowforge.admission.in.flight").gauge().value()).isEqualTo(1.0);
        }
        assertThat(meters.get("flowforge.admission.in.flight").gauge().value()).isZero();
    }

    @Test
    void validatesConfiguration() {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();

        assertThatThrownBy(() -> new WorkflowStartAdmissionGate(meters, 0, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WorkflowStartAdmissionGate(meters, 1, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
