package io.flowforge.controlplane;

import io.flowforge.application.recovery.DeadLetterLocation;
import io.flowforge.application.recovery.DeadLetterRecordNotFoundException;
import io.flowforge.application.recovery.DeadLetterReplayReceipt;
import io.flowforge.application.recovery.DeadLetterReplayService;
import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.kafka.KafkaConsumerRecoveryFactory;
import io.flowforge.messaging.FlowForgeHeaders;
import io.flowforge.messaging.FlowForgeTopics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "flowforge.kafka.enabled=true",
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.outbox.publisher-enabled=false",
        "flowforge.outbox.command-dispatch-enabled=false",
        "flowforge.results.consumer-enabled=false",
        "flowforge.leases.heartbeat-consumer-enabled=false",
        "flowforge.scheduling.enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false",
        "flowforge.leases.reaper-enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DeadLetterReplayIntegrationTest {
    private static final TenantId TENANT_A = new TenantId("replay-tenant-a");
    private static final TenantId TENANT_B = new TenantId("replay-tenant-b");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @DynamicPropertySource
    static void infrastructureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    DeadLetterReplayService service;

    @Autowired
    JdbcClient jdbc;

    @Test
    void replaysOneTenantOwnedRecordOnceAndPersistsOnlyOperationalEvidence() throws Exception {
        seedTenants();
        UUID eventId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        String payload = "{\"eventId\":\"" + eventId + "\",\"tenantId\":\"replay-tenant-a\"}";
        DeadLetterLocation location = publishDeadLetter(eventId, correlationId, payload);

        assertThatThrownBy(() -> service.inspect(TENANT_B, location))
                .isInstanceOf(DeadLetterRecordNotFoundException.class);

        var summary = service.inspect(TENANT_A, location);
        assertThat(summary.eventId()).isEqualTo(eventId.toString());
        assertThat(summary.tenantId()).isEqualTo(TENANT_A.value());
        assertThat(summary.payloadSha256()).hasSize(64);

        UUID idempotencyKey = UUID.randomUUID();
        DeadLetterReplayReceipt first = service.replay(
                TENANT_A, location, idempotencyKey, "incident-commander",
                "compatible consumer deployed"
        );
        assertThat(first.status()).isEqualTo(DeadLetterReplayReceipt.Status.PUBLISHED);

        try (KafkaConsumer<String, String> consumer = sourceConsumer()) {
            TopicPartition source = new TopicPartition(FlowForgeTopics.TASK_RESULTS_V1, 0);
            consumer.assign(List.of(source));
            consumer.seekToBeginning(List.of(source));
            var replayed = consumer.poll(Duration.ofSeconds(10)).records(source);
            assertThat(replayed).hasSize(1);
            assertThat(replayed.getFirst().value()).isEqualTo(payload);
            assertThat(header(replayed.getFirst(), FlowForgeHeaders.EVENT_ID)).isEqualTo(eventId.toString());
            assertThat(header(replayed.getFirst(), FlowForgeHeaders.TENANT_ID)).isEqualTo(TENANT_A.value());
            assertThat(replayed.getFirst().headers().lastHeader(
                    KafkaConsumerRecoveryFactory.DLQ_RECORD_ID_HEADER
            )).isNull();

            DeadLetterReplayReceipt duplicate = service.replay(
                    TENANT_A, location, idempotencyKey, "incident-commander",
                    "compatible consumer deployed"
            );
            assertThat(duplicate.status()).isEqualTo(DeadLetterReplayReceipt.Status.ALREADY_PUBLISHED);
            assertThat(consumer.poll(Duration.ofSeconds(2)).records(source)).isEmpty();
        }

        assertThat(jdbc.sql("""
                        SELECT status || ':' || attempt_count
                          FROM dlq_replay_request
                         WHERE tenant_id = :tenantId AND idempotency_key = :idempotencyKey
                        """)
                .param("tenantId", TENANT_A.value())
                .param("idempotencyKey", idempotencyKey)
                .query(String.class)
                .single()).isEqualTo("PUBLISHED:1");
        assertThat(jdbc.sql("""
                        SELECT count(*)
                          FROM information_schema.columns
                         WHERE table_schema = 'public'
                           AND table_name = 'dlq_replay_request'
                           AND column_name IN ('payload', 'record_key', 'headers')
                        """).query(Integer.class).single()).isZero();
    }

    private DeadLetterLocation publishDeadLetter(UUID eventId, UUID correlationId, String payload) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                FlowForgeTopics.TASK_RESULTS_DLQ_V1, 0, "execution-1", payload
        );
        add(record, FlowForgeHeaders.EVENT_ID, eventId.toString());
        add(record, FlowForgeHeaders.EVENT_TYPE, "TASK_RESULT_RECORDED");
        add(record, FlowForgeHeaders.SCHEMA_VERSION, "1");
        add(record, FlowForgeHeaders.CORRELATION_ID, correlationId.toString());
        add(record, FlowForgeHeaders.TENANT_ID, TENANT_A.value());
        add(record, KafkaConsumerRecoveryFactory.DLQ_SCHEMA_VERSION_HEADER, "1");
        add(record, KafkaConsumerRecoveryFactory.DLQ_RECORD_ID_HEADER, FlowForgeTopics.TASK_RESULTS_V1 + ":0:77");
        add(record, KafkaConsumerRecoveryFactory.DLQ_FAILURE_CLASS_HEADER, "example.FixedContractFailure");
        add(record, KafkaConsumerRecoveryFactory.DLQ_FAILED_AT_HEADER, Instant.now().minusSeconds(30).toString());

        try (KafkaProducer<String, String> producer = producer()) {
            var metadata = producer.send(record).get(10, TimeUnit.SECONDS);
            return new DeadLetterLocation(metadata.topic(), metadata.partition(), metadata.offset());
        }
    }

    private void seedTenants() {
        jdbc.sql("""
                INSERT INTO tenant_registry(tenant_id, display_name, status)
                VALUES ('replay-tenant-a', 'Replay tenant A', 'ACTIVE'),
                       ('replay-tenant-b', 'Replay tenant B', 'ACTIVE')
                ON CONFLICT DO NOTHING
                """).update();
    }

    private static KafkaProducer<String, String> producer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(properties);
    }

    private static KafkaConsumer<String, String> sourceConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-replay-acceptance-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<>(properties);
    }

    private static void add(ProducerRecord<String, String> record, String name, String value) {
        record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String header(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
