package io.flowforge.worker.config;

import io.flowforge.kafka.KafkaConsumerRecoveryFactory;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;

import java.time.Clock;
import java.time.Duration;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "flowforge.kafka", name = "enabled", havingValue = "true")
public class KafkaConsumerRecoveryConfiguration {
    @Bean
    DefaultErrorHandler flowForgeKafkaErrorHandler(
            KafkaTemplate<String, String> kafka,
            MeterRegistry meters,
            Clock clock,
            @Value("${flowforge.kafka.recovery.max-retries:2}") int maxRetries,
            @Value("${flowforge.kafka.recovery.initial-backoff:250ms}") Duration initialBackoff,
            @Value("${flowforge.kafka.recovery.backoff-multiplier:2.0}") double multiplier,
            @Value("${flowforge.kafka.recovery.max-backoff:2s}") Duration maxBackoff,
            @Value("${flowforge.kafka.recovery.publish-timeout:10s}") Duration publishTimeout
    ) {
        return KafkaConsumerRecoveryFactory.create(
                kafka, meters, clock, maxRetries, initialBackoff, multiplier, maxBackoff, publishTimeout
        );
    }
}
