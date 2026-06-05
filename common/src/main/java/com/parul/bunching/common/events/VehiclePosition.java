package com.parul.bunching.common.events;

import java.time.Instant;

/**
 * One position report for one bus, as published to {@code mbta.vehicle-positions}.
 *
 * @param vehicleId           MBTA vehicle ID
 * @param label               the number painted on the bus
 * @param routeId             route the bus is serving (also the Kafka message key)
 * @param directionId         0 or 1, the direction of travel on the route
 * @param tripId              current trip
 * @param stopId              stop the bus is at or heading to
 * @param currentStopSequence position of that stop on the trip
 * @param currentStatus       INCOMING_AT, STOPPED_AT or IN_TRANSIT_TO
 * @param latitude            WGS84 latitude
 * @param longitude           WGS84 longitude
 * @param updatedAt           when the MBTA last updated this position (event time)
 */
public record VehiclePosition(
        String vehicleId,
        String label,
        String routeId,
        int directionId,
        String tripId,
        String stopId,
        Integer currentStopSequence,
        String currentStatus,
        double latitude,
        double longitude,
        Instant updatedAt) {

    /** Route plus direction: two buses only bunch if they travel the same way on the same route. */
    public String routeDirectionKey() {
        return routeId + ":" + directionId;
    }
}
