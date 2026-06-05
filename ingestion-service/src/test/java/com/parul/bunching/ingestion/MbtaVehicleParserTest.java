package com.parul.bunching.ingestion;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.parul.bunching.common.events.VehiclePosition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class MbtaVehicleParserTest {

    private final MbtaVehicleParser parser = new MbtaVehicleParser(new ObjectMapper());

    @Test
    void parsesValidVehiclesAndSkipsOnesWithoutRouteOrLocation() throws IOException {
        String json = new String(getClass().getResourceAsStream("/vehicles-sample.json").readAllBytes(),
                StandardCharsets.UTF_8);

        List<VehiclePosition> positions = parser.parse(json);

        assertThat(positions).hasSize(1);
        VehiclePosition bus = positions.getFirst();
        assertThat(bus.vehicleId()).isEqualTo("y1871");
        assertThat(bus.label()).isEqualTo("1871");
        assertThat(bus.routeId()).isEqualTo("39");
        assertThat(bus.directionId()).isEqualTo(1);
        assertThat(bus.tripId()).isEqualTo("70123456");
        assertThat(bus.stopId()).isEqualTo("1355");
        assertThat(bus.currentStopSequence()).isEqualTo(14);
        assertThat(bus.latitude()).isEqualTo(42.3497);
        assertThat(bus.updatedAt()).isEqualTo(Instant.parse("2026-10-06T12:15:30Z"));
    }

    @Test
    void returnsEmptyListForMalformedResponse() {
        assertThat(parser.parse("not json")).isEmpty();
    }
}
