package com.parul.bunching.common;

/** Kafka topic names shared by every service. */
public final class Topics {

    /** Raw vehicle positions from the MBTA feed, keyed by route ID. */
    public static final String VEHICLE_POSITIONS = "mbta.vehicle-positions";

    /** Events the detector could not process (bad payloads, invalid coordinates). */
    public static final String VEHICLE_POSITIONS_DLT = "mbta.vehicle-positions.DLT";

    /** Bunching alerts produced by the detector, keyed by route ID. */
    public static final String BUNCHING_ALERTS = "mbta.bunching-alerts";

    private Topics() {
    }
}
