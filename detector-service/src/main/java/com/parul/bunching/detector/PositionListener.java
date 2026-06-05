package com.parul.bunching.detector;

import com.parul.bunching.common.Topics;
import com.parul.bunching.common.events.BunchingAlert;
import com.parul.bunching.common.events.VehiclePosition;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

/**
 * Feeds every vehicle position into the detector and publishes any alerts it finds.
 *
 * <p>Alerts are sent synchronously: the listener waits for Kafka to acknowledge the alert before
 * returning, so the position's offset is only committed after its alert is safely stored. If the
 * service crashes in between, the position is re-read on restart (at-least-once).
 */
@Component
public class PositionListener implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(PositionListener.class);

    private final BunchingDetector detector;
    private final KafkaTemplate<String, BunchingAlert> alertTemplate;
    private final DetectorProperties properties;
    private final Set<TopicPartition> rewound = ConcurrentHashMap.newKeySet();

    public PositionListener(BunchingDetector detector, KafkaTemplate<String, BunchingAlert> alertTemplate,
                            DetectorProperties properties) {
        this.detector = detector;
        this.alertTemplate = alertTemplate;
        this.properties = properties;
    }

    @KafkaListener(topics = Topics.VEHICLE_POSITIONS)
    public void onPosition(VehiclePosition position) throws Exception {
        for (BunchingAlert alert : detector.onPosition(position)) {
            alertTemplate.send(Topics.BUNCHING_ALERTS, alert.routeId(), alert).get(10, TimeUnit.SECONDS);
            log.info("BUNCHING route {} dir {}: buses {} and {} are {} m apart",
                    alert.routeId(), alert.directionId(), alert.leadVehicle(), alert.trailingVehicle(),
                    alert.distanceMeters());
        }
    }

    /** Replay mode: rewind each partition once, the first time this instance is assigned it. */
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        if (!properties.replayFromBeginning()) {
            return;
        }
        for (TopicPartition partition : assignments.keySet()) {
            if (rewound.add(partition)) {
                log.info("Replay mode: rewinding {} to the beginning", partition);
                callback.seekToBeginning(Set.of(partition));
            }
        }
    }
}
