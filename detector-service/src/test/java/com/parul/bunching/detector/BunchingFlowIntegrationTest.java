package com.parul.bunching.detector;

import static com.parul.bunching.detector.BunchingDetectorTest.bus;
import static org.assertj.core.api.Assertions.assertThat;

import com.parul.bunching.common.Topics;
import com.parul.bunching.common.events.VehiclePosition;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Runs the real detector against a real Kafka broker in Docker: positions go in one topic,
 * alerts and dead letters must come out of the others.
 */
@SpringBootTest
@Testcontainers
class BunchingFlowIntegrationTest {

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    @Autowired
    KafkaTemplate<String, Object> template;

    /** In production the ingestion service creates this topic; here the test does. */
    @TestConfiguration
    static class TopicSetup {
        @Bean
        NewTopic vehiclePositions() {
            return TopicBuilder.name(Topics.VEHICLE_POSITIONS).partitions(3).replicas(1).build();
        }
    }

    @Test
    void closeBusesProduceAnAlertAndBadEventsGoToTheDlt() throws Exception {
        Instant now = Instant.parse("2026-10-06T12:00:00Z");
        template.send(Topics.VEHICLE_POSITIONS, "39", bus("A", "39", 1, 10, 42.3497, -71.0773, now)).get();
        template.send(Topics.VEHICLE_POSITIONS, "39", bus("B", "39", 1, 9, 42.3500, -71.0773, now)).get();

        VehiclePosition invalid = bus("C", "39", 1, 10, 999, -71.0773, now);
        template.send(Topics.VEHICLE_POSITIONS, "39", invalid).get();
        try (KafkaProducer<String, String> raw = new KafkaProducer<>(
                Map.<String, Object>of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
                new StringSerializer(), new StringSerializer())) {
            raw.send(new ProducerRecord<>(Topics.VEHICLE_POSITIONS, "39", "{not valid json")).get();
        }

        List<ConsumerRecord<String, String>> alerts = readAll(Topics.BUNCHING_ALERTS, 1);
        assertThat(alerts).hasSize(1);
        assertThat(alerts.getFirst().key()).isEqualTo("39");
        assertThat(alerts.getFirst().value()).contains("\"leadVehicle\":\"A\"");

        List<ConsumerRecord<String, String>> deadLetters = readAll(Topics.VEHICLE_POSITIONS_DLT, 2);
        assertThat(deadLetters).hasSize(2);
        assertThat(deadLetters).allSatisfy(r ->
                assertThat(r.headers().lastHeader("kafka_dlt-exception-message")).isNotNull());
    }

    private List<ConsumerRecord<String, String>> readAll(String topic, int expected) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (records.size() < expected && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
            // Short extra wait to catch anything unexpected beyond what we asked for.
            consumer.poll(Duration.ofSeconds(2)).forEach(records::add);
        }
        return records;
    }
}
