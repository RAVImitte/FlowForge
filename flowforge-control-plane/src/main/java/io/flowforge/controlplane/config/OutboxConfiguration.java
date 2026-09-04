package io.flowforge.controlplane.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({OutboxProperties.class, ResultIngestionProperties.class})
public class OutboxConfiguration {
}
