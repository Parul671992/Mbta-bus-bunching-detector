package com.parul.bunching.detector;

import com.parul.bunching.common.events.BunchingAlert;
import com.parul.bunching.common.events.VehiclePosition;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Keeps the latest position of every bus, grouped by route and direction, and reports pairs
 * of buses that are running too close together.
 *
 * <p>All timing (staleness, cooldown) uses the event's own {@code updatedAt}, never the wall
 * clock. That is what makes replay correct: re-reading yesterday's data produces the same
 * alerts it would have produced live.
 *
 * <p>This state lives in memory. It works because positions are keyed by route ID, so every
 * position for a route arrives on the same partition and therefore at the same consumer.
 */
@Component
public class BunchingDetector {

    private final DetectorProperties properties;

    /** routeId:directionId -> vehicleId -> latest position. */
    private final Map<String, Map<String, VehiclePosition>> byRouteDirection = new HashMap<>();

    /** vehicleId -> the group it is currently in, so a bus that changes direction leaves its old group. */
    private final Map<String, String> groupOfVehicle = new HashMap<>();

    /** pair key -> when that pair was last alerted. */
    private final Map<String, Instant> lastAlerted = new HashMap<>();

    public BunchingDetector(DetectorProperties properties) {
        this.properties = properties;
    }

    public synchronized List<BunchingAlert> onPosition(VehiclePosition position) {
        validate(position);

        String groupKey = position.routeDirectionKey();
        String previousGroup = groupOfVehicle.put(position.vehicleId(), groupKey);
        if (previousGroup != null && !previousGroup.equals(groupKey)) {
            Map<String, VehiclePosition> oldGroup = byRouteDirection.get(previousGroup);
            if (oldGroup != null) {
                oldGroup.remove(position.vehicleId());
            }
        }

        Map<String, VehiclePosition> group = byRouteDirection.computeIfAbsent(groupKey, k -> new HashMap<>());
        group.put(position.vehicleId(), position);

        Instant now = position.updatedAt();
        evictStale(group, now);

        // A bus waiting at the first stop of its trip is on layover, not bunched.
        if (isOnLayover(position)) {
            return List.of();
        }

        List<BunchingAlert> alerts = new ArrayList<>();
        for (VehiclePosition other : group.values()) {
            if (other.vehicleId().equals(position.vehicleId()) || isOnLayover(other)) {
                continue;
            }
            double distance = Geo.distanceMeters(position.latitude(), position.longitude(),
                    other.latitude(), other.longitude());
            if (distance <= properties.thresholdMeters() && cooldownElapsed(groupKey, position, other, now)) {
                alerts.add(toAlert(position, other, distance));
            }
        }
        return alerts;
    }

    private static void validate(VehiclePosition p) {
        if (p.routeId() == null || p.vehicleId() == null || p.updatedAt() == null) {
            throw new InvalidVehicleEventException("Missing route, vehicle or timestamp: " + p);
        }
        if (Math.abs(p.latitude()) > 90 || Math.abs(p.longitude()) > 180) {
            throw new InvalidVehicleEventException("Coordinates out of range: " + p);
        }
    }

    private void evictStale(Map<String, VehiclePosition> group, Instant now) {
        Instant cutoff = now.minus(properties.staleAfter());
        group.values().removeIf(p -> p.updatedAt().isBefore(cutoff));
    }

    private static boolean isOnLayover(VehiclePosition p) {
        return p.currentStopSequence() != null && p.currentStopSequence() <= 1;
    }

    private boolean cooldownElapsed(String groupKey, VehiclePosition a, VehiclePosition b, Instant now) {
        String first = a.vehicleId().compareTo(b.vehicleId()) < 0 ? a.vehicleId() : b.vehicleId();
        String second = first.equals(a.vehicleId()) ? b.vehicleId() : a.vehicleId();
        String pairKey = groupKey + "|" + first + "|" + second;

        Instant last = lastAlerted.get(pairKey);
        if (last != null && now.isBefore(last.plus(properties.cooldown()))) {
            return false;
        }
        lastAlerted.put(pairKey, now);
        if (lastAlerted.size() > 10_000) {
            Instant cutoff = now.minus(properties.cooldown());
            lastAlerted.values().removeIf(t -> t.isBefore(cutoff));
        }
        return true;
    }

    /** The bus further along its trip (higher stop sequence) is the lead bus. */
    private static BunchingAlert toAlert(VehiclePosition a, VehiclePosition b, double distance) {
        int seqA = a.currentStopSequence() == null ? 0 : a.currentStopSequence();
        int seqB = b.currentStopSequence() == null ? 0 : b.currentStopSequence();
        VehiclePosition lead = seqA >= seqB ? a : b;
        VehiclePosition trailing = lead == a ? b : a;
        return new BunchingAlert(
                a.routeId(),
                a.directionId(),
                labelOf(lead),
                labelOf(trailing),
                Math.round(distance * 10) / 10.0,
                (a.latitude() + b.latitude()) / 2,
                (a.longitude() + b.longitude()) / 2,
                a.updatedAt());
    }

    private static String labelOf(VehiclePosition p) {
        return p.label() != null ? p.label() : p.vehicleId();
    }
}
