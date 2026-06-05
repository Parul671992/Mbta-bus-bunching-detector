package com.parul.bunching.common.events;

import java.time.Instant;

/**
 * Two buses on the same route and direction were found running too close together.
 *
 * @param routeId         route the buses are serving (also the Kafka message key)
 * @param directionId     direction of travel
 * @param leadVehicle     label of the first bus
 * @param trailingVehicle label of the second bus
 * @param distanceMeters  straight-line distance between them when detected
 * @param latitude        midpoint latitude, for showing the alert on a map
 * @param longitude       midpoint longitude
 * @param detectedAt      event time of the position report that triggered the alert
 */
public record BunchingAlert(
        String routeId,
        int directionId,
        String leadVehicle,
        String trailingVehicle,
        double distanceMeters,
        double latitude,
        double longitude,
        Instant detectedAt) {
}
