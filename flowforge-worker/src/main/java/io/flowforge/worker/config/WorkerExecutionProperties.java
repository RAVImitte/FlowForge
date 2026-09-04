package io.flowforge.worker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("flowforge.worker.execution")
public record WorkerExecutionProperties(
        int concurrency,
        int resultBatchSize,
        Duration resultLeaseDuration,
        Duration resultPublishTimeout
) {
    public WorkerExecutionProperties {
        if (concurrency < 1 || concurrency > 100) {
            throw new IllegalArgumentException("Worker concurrency must be between 1 and 100");
        }
        if (resultBatchSize < 1 || resultBatchSize > 1_000) {
            throw new IllegalArgumentException("Result batch size must be between 1 and 1000");
        }
        if (resultLeaseDuration == null || resultLeaseDuration.isZero() || resultLeaseDuration.isNegative()) {
            throw new IllegalArgumentException("Result lease duration must be positive");
        }
        if (resultPublishTimeout == null || resultPublishTimeout.isZero() || resultPublishTimeout.isNegative()) {
            throw new IllegalArgumentException("Result publish timeout must be positive");
        }
    }
}
