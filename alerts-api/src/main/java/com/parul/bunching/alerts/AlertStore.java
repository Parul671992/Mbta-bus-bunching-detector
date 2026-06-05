package com.parul.bunching.alerts;

import com.parul.bunching.common.events.BunchingAlert;
import com.parul.bunching.common.events.VehiclePosition;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/**
 * In-memory view built from the two topics. Kafka is the source of truth: on restart the
 * listeners re-read the topics and rebuild this state.
 */
@Component
public class AlertStore {

    private static final int MAX_ALERTS = 500;
    private static final Duration LIVE_WINDOW = Duration.ofMinutes(5);

    private final Deque<BunchingAlert> recentAlerts = new ConcurrentLinkedDeque<>();
    private final Map<String, AtomicInteger> alertCountByRoute = new ConcurrentHashMap<>();
    private final Map<String, VehiclePosition> latestByVehicle = new ConcurrentHashMap<>();

    public void addAlert(BunchingAlert alert) {
        recentAlerts.addFirst(alert);
        while (recentAlerts.size() > MAX_ALERTS) {
            recentAlerts.pollLast();
        }
        alertCountByRoute.computeIfAbsent(alert.routeId(), r -> new AtomicInteger()).incrementAndGet();
    }

    public void updateVehicle(VehiclePosition position) {
        latestByVehicle.merge(position.vehicleId(), position,
                (old, incoming) -> incoming.updatedAt().isAfter(old.updatedAt()) ? incoming : old);
    }

    public List<BunchingAlert> alerts(String route, int limit) {
        return recentAlerts.stream()
                .filter(a -> route == null || route.equals(a.routeId()))
                .limit(limit)
                .toList();
    }

    /** Routes ordered by how often they bunch: the most useful single view of the data. */
    public Map<String, Integer> alertCountsByRoute() {
        return alertCountByRoute.entrySet().stream()
                .sorted(Map.Entry.<String, AtomicInteger>comparingByValue(
                        Comparator.comparingInt(AtomicInteger::get)).reversed())
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(),
                        (a, b) -> a, LinkedHashMap::new));
    }

    public List<VehiclePosition> liveVehicles(String route) {
        Instant cutoff = latestByVehicle.values().stream()
                .map(VehiclePosition::updatedAt)
                .max(Comparator.naturalOrder())
                .orElse(Instant.now())
                .minus(LIVE_WINDOW);
        return latestByVehicle.values().stream()
                .filter(v -> v.updatedAt().isAfter(cutoff))
                .filter(v -> route == null || route.equals(v.routeId()))
                .toList();
    }
}
