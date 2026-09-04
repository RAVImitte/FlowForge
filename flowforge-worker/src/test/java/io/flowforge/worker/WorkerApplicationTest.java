package io.flowforge.worker;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "flowforge.kafka.enabled=false"
)
@Testcontainers(disabledWithoutDocker = true)
class WorkerApplicationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    ApplicationContext context;

    @Test
    void startsAsAnIndependentWorkerProcess() {
        HealthIndicator worker = context.getBean("worker", HealthIndicator.class);

        assertThat(worker.health().getStatus().getCode()).isEqualTo("UP");
        assertThat(worker.health().getDetails()).containsEntry("consumerGroup", "flowforge-workers-v1");
        assertThat(context.containsBean("kafka")).isFalse();
    }
}
