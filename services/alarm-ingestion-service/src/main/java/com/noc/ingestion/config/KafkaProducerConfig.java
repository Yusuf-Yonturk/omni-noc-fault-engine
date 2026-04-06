package com.noc.ingestion.config;

import com.noc.ingestion.event.AlarmNormalizedEvent;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/** Kafka producer configuration: idempotent producer, JSON serialization, topic definitions. */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    // ── Producer Factory ─────────────────────────────────────────────────────

    /** Idempotent Kafka producer with JSON serialization and acks=all. */
    @Bean
    public ProducerFactory<String, AlarmNormalizedEvent> alarmNormalizedEventProducerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Reliability settings
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        // Performance tuning
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // Disable Spring type headers to avoid consumer deserialization issues
        config.put(JsonSerializer.ADD_TYPE_HEADERS, false);

        return new DefaultKafkaProducerFactory<>(config);
    }

    /** KafkaTemplate for publishing AlarmNormalizedEvent; keyed by siteId. */
    @Bean
    public KafkaTemplate<String, AlarmNormalizedEvent> alarmNormalizedEventKafkaTemplate() {
        return new KafkaTemplate<>(alarmNormalizedEventProducerFactory());
    }

    // ── Topic Administration ─────────────────────────────────────────────────

    /** KafkaAdmin for programmatic topic creation and management. */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(config);
    }

    /** alarm.normalized topic — 6 partitions for parallel consumption, keyed by siteId. */
    @Bean
    public NewTopic alarmNormalizedTopic() {
        return TopicBuilder.name("alarm.normalized")
                .partitions(6)
                .replicas(1)
                .config("retention.ms", String.valueOf(7 * 24 * 60 * 60 * 1000L)) // 7 days
                .config("cleanup.policy", "delete")
                .build();
    }

    /** Declares incident.updated topic so startup doesn't fail if it doesn't exist yet. */
    @Bean
    public NewTopic incidentUpdatedTopic() {
        return TopicBuilder.name("incident.updated")
                .partitions(4)
                .replicas(1)
                .build();
    }

    /** incident.status.changed topic consumed by notification-service. */
    @Bean
    public NewTopic incidentStatusChangedTopic() {
        return TopicBuilder.name("incident.status.changed")
                .partitions(4)
                .replicas(1)
                .build();
    }

    /** incident.escalated topic consumed by notification-service. */
    @Bean
    public NewTopic incidentEscalatedTopic() {
        return TopicBuilder.name("incident.escalated")
                .partitions(4)
                .replicas(1)
                .build();
    }
}
