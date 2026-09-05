package io.flowforge.controlplane.adapter.out.execution;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerRetryLifecycleObserverTest {
    @AfterEach
    void clearTransactionSynchronization() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void publishesRetryMetricsOnlyAfterTheStateTransactionCommits() {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        MicrometerRetryLifecycleObserver observer = new MicrometerRetryLifecycleObserver(meters);
        TransactionSynchronizationManager.initSynchronization();

        observer.scheduled(Duration.ofSeconds(2));
        observer.started();
        observer.exhausted();
        observer.deadLettered();

        assertThat(meters.find("flowforge.retries.scheduled").counter()).isNull();
        assertThat(meters.find("flowforge.retries.started").counter()).isNull();
        assertThat(meters.find("flowforge.retries.exhausted").counter()).isNull();
        assertThat(meters.find("flowforge.tasks.dead.lettered").counter()).isNull();
        assertThat(meters.get("flowforge.retries.backoff").summary().count()).isZero();

        TransactionSynchronizationManager.getSynchronizations()
                .forEach(synchronization -> synchronization.afterCommit());

        assertThat(meters.get("flowforge.retries.scheduled").counter().count()).isEqualTo(1.0);
        assertThat(meters.get("flowforge.retries.started").counter().count()).isEqualTo(1.0);
        assertThat(meters.get("flowforge.retries.exhausted").counter().count()).isEqualTo(1.0);
        assertThat(meters.get("flowforge.tasks.dead.lettered").counter().count()).isEqualTo(1.0);
        assertThat(meters.get("flowforge.retries.backoff").summary().count()).isEqualTo(1);
        assertThat(meters.get("flowforge.retries.backoff").summary().totalAmount()).isEqualTo(2_000.0);
    }
}
