package io.flowforge.worker.health;

import io.flowforge.worker.config.WorkerProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.Health;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component("kafka")
@ConditionalOnProperty(prefix = "flowforge.kafka", name = "enabled", havingValue = "true")
public class KafkaClusterHealthIndicator extends AbstractHealthIndicator {
    private final KafkaAdmin kafkaAdmin;
    private final WorkerProperties properties;

    public KafkaClusterHealthIndicator(KafkaAdmin kafkaAdmin, WorkerProperties properties) {
        super("Kafka cluster health check failed");
        this.kafkaAdmin = kafkaAdmin;
        this.properties = properties;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        long timeoutMillis = properties.kafkaHealthTimeout().toMillis();
        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            DescribeClusterResult cluster = client.describeCluster();
            builder.up()
                    .withDetail("clusterId", cluster.clusterId().get(timeoutMillis, TimeUnit.MILLISECONDS))
                    .withDetail("nodes", cluster.nodes().get(timeoutMillis, TimeUnit.MILLISECONDS).size());
        }
    }
}
