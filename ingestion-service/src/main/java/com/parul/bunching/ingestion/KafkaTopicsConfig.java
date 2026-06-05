package com.parul.bunching.ingestion;

import com.parul.bunching.common.Topics;
import java.time.Duration;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    /**
     * Three partitions so up to three detector instances can share the work.
     * Positions are kept for 24 hours, which is what makes replaying a day of data possible.
     */
    @Bean
    NewTopic vehiclePositionsTopic() {
        return TopicBuilder.name(Topics.VEHICLE_POSITIONS)
                .partitions(3)
                .replicas(1)
                .config("retention.ms", String.valueOf(Duration.ofHours(24).toMillis()))
                .build();
    }
}
