package io.flowforge.controlplane.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("flowforge.results")
public record ResultIngestionProperties(int enqueueBatchSize, boolean inlineDispatchEnabled) {
    public ResultIngestionProperties {
        if (enqueueBatchSize < 1 || enqueueBatchSize > 1_000) {
            throw new IllegalArgumentException("Result enqueue batch size must be between 1 and 1000");
        }
    }
}
