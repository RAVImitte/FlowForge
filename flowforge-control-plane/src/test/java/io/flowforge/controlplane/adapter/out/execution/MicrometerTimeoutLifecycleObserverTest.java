package io.flowforge.controlplane.adapter.out.execution;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerTimeoutLifecycleObserverTest {
    @AfterEach
    void clearTransactionSynchronization() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void publishesTimeoutMetricsOnlyAfterTheStateTransactionCommits() {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        MicrometerTimeoutLifecycleObserver observer = new MicrometerTimeoutLifecycleObserver(meters);
        TransactionSynchronizationManager.initSynchronization();

        observer.attemptTimedOut(false, true);

        assertThat(meters.find("flowforge.timeouts.tasks").counter()).isNull();
        assertThat(meters.find("flowforge.timeouts.terminal").counter()).isNull();
        assertThat(meters.find("flowforge.timeouts.workflows.failed").counter()).isNull();

        TransactionSynchronizationManager.getSynchronizations()
                .forEach(synchronization -> synchronization.afterCommit());

        assertThat(meters.get("flowforge.timeouts.tasks").counter().count()).isEqualTo(1.0);
        assertThat(meters.get("flowforge.timeouts.terminal").counter().count()).isEqualTo(1.0);
        assertThat(meters.get("flowforge.timeouts.workflows.failed").counter().count()).isEqualTo(1.0);
    }
}
