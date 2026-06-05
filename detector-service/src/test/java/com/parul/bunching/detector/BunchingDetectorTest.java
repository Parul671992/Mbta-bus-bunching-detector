package com.parul.bunching.detector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.parul.bunching.common.events.BunchingAlert;
import com.parul.bunching.common.events.VehiclePosition;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BunchingDetectorTest {

    // Near Copley on Huntington Ave. 0.0009 degrees of latitude is roughly 100 m.
    private static final double LAT = 42.3497;
    private static final double LON = -71.0773;
    private static final Instant T0 = Instant.parse("2026-10-06T12:00:00Z");

    private BunchingDetector detector;

    @BeforeEach
    void setUp() {
        detector = new BunchingDetector(new DetectorProperties(300, 120, 10, false));
    }

    @Test
    void alertsWhenTwoBusesOnSameRouteAndDirectionAreClose() {
        detector.onPosition(bus("A", "39", 1, 10, LAT, LON, T0));
        List<BunchingAlert> alerts = detector.onPosition(bus("B", "39", 1, 9, LAT + 0.0009, LON, T0.plusSeconds(5)));

        assertThat(alerts).hasSize(1);
        BunchingAlert alert = alerts.getFirst();
        assertThat(alert.routeId()).isEqualTo("39");
        assertThat(alert.leadVehicle()).isEqualTo("A");
        assertThat(alert.trailingVehicle()).isEqualTo("B");
        assertThat(alert.distanceMeters()).isBetween(90.0, 110.0);
    }

    @Test
    void ignoresBusesThatAreFarApart() {
        detector.onPosition(bus("A", "39", 1, 10, LAT, LON, T0));
        assertThat(detector.onPosition(bus("B", "39", 1, 5, LAT + 0.01, LON, T0))).isEmpty();
    }

    @Test
    void ignoresBusesGoingOppositeDirections() {
        detector.onPosition(bus("A", "39", 0, 10, LAT, LON, T0));
        assertThat(detector.onPosition(bus("B", "39", 1, 10, LAT, LON, T0))).isEmpty();
    }

    @Test
    void ignoresBusesOnDifferentRoutes() {
        detector.onPosition(bus("A", "39", 1, 10, LAT, LON, T0));
        assertThat(detector.onPosition(bus("B", "1", 1, 10, LAT, LON, T0))).isEmpty();
    }

    @Test
    void ignoresBusesWaitingAtTheFirstStop() {
        detector.onPosition(bus("A", "39", 1, 1, LAT, LON, T0));
        assertThat(detector.onPosition(bus("B", "39", 1, 1, LAT, LON, T0))).isEmpty();
    }

    @Test
    void doesNotRepeatAlertForSamePairWithinCooldown() {
        detector.onPosition(bus("A", "39", 1, 10, LAT, LON, T0));
        assertThat(detector.onPosition(bus("B", "39", 1, 9, LAT, LON, T0))).hasSize(1);

        assertThat(detector.onPosition(bus("B", "39", 1, 9, LAT, LON, T0.plusSeconds(60)))).isEmpty();
    }

    @Test
    void alertsAgainOnceCooldownHasPassed() {
        detector.onPosition(bus("A", "39", 1, 10, LAT, LON, T0));
        detector.onPosition(bus("B", "39", 1, 9, LAT, LON, T0));

        Instant later = T0.plusSeconds(11 * 60);
        detector.onPosition(bus("A", "39", 1, 20, LAT, LON, later));
        assertThat(detector.onPosition(bus("B", "39", 1, 19, LAT, LON, later))).hasSize(1);
    }

    @Test
    void forgetsBusesThatStoppedReporting() {
        detector.onPosition(bus("A", "39", 1, 10, LAT, LON, T0));
        // Three minutes later A has gone quiet, so B passing the same spot is not bunching.
        assertThat(detector.onPosition(bus("B", "39", 1, 10, LAT, LON, T0.plusSeconds(180)))).isEmpty();
    }

    @Test
    void rejectsImpossibleCoordinates() {
        assertThatThrownBy(() -> detector.onPosition(bus("A", "39", 1, 10, 999, LON, T0)))
                .isInstanceOf(InvalidVehicleEventException.class);
    }

    static VehiclePosition bus(String id, String route, int direction, Integer stopSeq,
                               double lat, double lon, Instant at) {
        return new VehiclePosition(id, id, route, direction, "trip-" + id, "stop", stopSeq,
                "IN_TRANSIT_TO", lat, lon, at);
    }
}
