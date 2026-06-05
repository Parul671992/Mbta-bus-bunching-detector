package com.parul.bunching.detector;

import com.parul.bunching.common.Topics;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    NewTopic bunchingAlertsTopic() {
        return TopicBuilder.name(Topics.BUNCHING_ALERTS)
                .partitions(3)
                .replicas(1)
                .config("retention.ms", String.valueOf(Duration.ofDays(7).toMillis()))
                .build();
    }

    @Bean
    NewTopic vehiclePositionsDltTopic() {
        return TopicBuilder.name(Topics.VEHICLE_POSITIONS_DLT)
                .partitions(1)
                .replicas(1)
                .config("retention.ms", String.valueOf(Duration.ofDays(7).toMillis()))
                .build();
    }

    /**
     * Error handling for the position listener.
     *
     * <ul>
     *   <li>Unexpected failures are retried twice, one second apart.</li>
     *   <li>Invalid events and unreadable JSON are not retried: they can never succeed.</li>
     *   <li>Either way the record ends up on the DLT with the error in its headers, and the
     *       consumer moves on instead of getting stuck on one bad message.</li>
     * </ul>
     *
     * Spring Boot attaches this bean to the listener container automatically.
     */
    @Bean
    DefaultErrorHandler errorHandler(ProducerFactory<?, ?> producerFactory) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(dltTemplate(producerFactory),
                (record, exception) -> new TopicPartition(Topics.VEHICLE_POSITIONS_DLT, 0));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
        handler.addNotRetryableExceptions(InvalidVehicleEventException.class);
        return handler;
    }

    /**
     * Template used only for the DLT. Records that failed deserialization arrive as raw bytes,
     * everything else as objects, so the value serializer picks by type.
     * Deliberately not a bean, so Spring Boot still auto-configures the main KafkaTemplate.
     *
     * The settings are copied from the producer factory Spring Boot already built, so the DLT
     * producer always connects to the same broker as everything else: the docker-compose address
     * in Docker, or the Testcontainers broker in tests.
     */
    private static KafkaTemplate<Object, Object> dltTemplate(ProducerFactory<?, ?> producerFactory) {
        Map<Class<?>, Serializer<?>> byType = new LinkedHashMap<>();
        byType.put(byte[].class, new ByteArraySerializer());
        byType.put(Object.class, new JsonSerializer<>());
        Map<String, Object> config = new HashMap<>(producerFactory.getConfigurationProperties());
        DefaultKafkaProducerFactory<Object, Object> factory = new DefaultKafkaProducerFactory<>(config,
                castSerializer(new StringSerializer()), new DelegatingByTypeSerializer(byType, true));
        return new KafkaTemplate<>(factory);
    }

    @SuppressWarnings("unchecked")
    private static Serializer<Object> castSerializer(Serializer<?> serializer) {
        return (Serializer<Object>) serializer;
    }
}
