package com.parul.bunching.alerts;

import com.parul.bunching.common.Topics;
import com.parul.bunching.common.events.BunchingAlert;
import com.parul.bunching.common.events.VehiclePosition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Two listeners in two different consumer groups.
 *
 * <p>{@code alerts-api-live} reads the same {@code mbta.vehicle-positions} topic as the detector,
 * but because it is a separate group it gets its own copy of every message and its own offsets.
 * The detector and this API never compete for messages or slow each other down.
 */
@Component
public class TopicListeners {

    private final AlertStore store;

    public TopicListeners(AlertStore store) {
        this.store = store;
    }

    @KafkaListener(topics = Topics.BUNCHING_ALERTS, groupId = "alerts-api",
            properties = "spring.json.value.default.type=com.parul.bunching.common.events.BunchingAlert")
    public void onAlert(BunchingAlert alert) {
        store.addAlert(alert);
    }

    @KafkaListener(topics = Topics.VEHICLE_POSITIONS, groupId = "alerts-api-live",
            properties = {
                    "auto.offset.reset=latest",
                    "spring.json.value.default.type=com.parul.bunching.common.events.VehiclePosition"
            })
    public void onPosition(VehiclePosition position) {
        store.updateVehicle(position);
    }
}
