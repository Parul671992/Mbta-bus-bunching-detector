package com.parul.bunching.ingestion;

import com.parul.bunching.common.Topics;
import com.parul.bunching.common.events.VehiclePosition;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Polls the MBTA feed on a fixed delay and publishes each new position to Kafka.
 *
 * <p>Messages are keyed by route ID, so every position for a route lands on the same partition
 * and reaches the detector in order. A position is only published when the MBTA has updated it
 * since the last poll, so idle buses don't flood the topic with duplicates.
 */
@Component
public class VehiclePoller {

    private static final Logger log = LoggerFactory.getLogger(VehiclePoller.class);

    private final MbtaClient client;
    private final MbtaVehicleParser parser;
    private final KafkaTemplate<String, VehiclePosition> kafkaTemplate;
    private final Map<String, Instant> lastPublished = new ConcurrentHashMap<>();

    public VehiclePoller(MbtaClient client, MbtaVehicleParser parser,
                         KafkaTemplate<String, VehiclePosition> kafkaTemplate) {
        this.client = client;
        this.parser = parser;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${mbta.poll-interval-ms}", initialDelay = 2000)
    public void poll() {
        List<VehiclePosition> positions;
        try {
            positions = parser.parse(client.fetchVehicles());
        } catch (Exception e) {
            // A failed poll is not fatal: the next one runs in a few seconds.
            log.warn("MBTA poll failed: {}", e.getMessage());
            return;
        }

        int published = 0;
        for (VehiclePosition position : positions) {
            if (isNew(position)) {
                kafkaTemplate.send(Topics.VEHICLE_POSITIONS, position.routeId(), position);
                published++;
            }
        }
        log.info("Polled {} vehicles, published {} new positions", positions.size(), published);
    }

    boolean isNew(VehiclePosition position) {
        Instant previous = lastPublished.put(position.vehicleId(), position.updatedAt());
        return previous == null || position.updatedAt().isAfter(previous);
    }
}
