package io.flowforge.worker.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
public class WorkerConfiguration {
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
