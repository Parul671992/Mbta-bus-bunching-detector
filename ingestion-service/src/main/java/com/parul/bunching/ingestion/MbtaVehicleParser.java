package com.parul.bunching.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.parul.bunching.common.events.VehiclePosition;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Turns an MBTA {@code /vehicles} JSON:API response into {@link VehiclePosition} events.
 * Vehicles without a route or a location (for example buses parked in a garage) are skipped.
 */
@Component
public class MbtaVehicleParser {

    private static final Logger log = LoggerFactory.getLogger(MbtaVehicleParser.class);

    private final ObjectMapper objectMapper;

    public MbtaVehicleParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public List<VehiclePosition> parse(String json) {
        List<VehiclePosition> positions = new ArrayList<>();
        JsonNode data;
        try {
            data = objectMapper.readTree(json).path("data");
        } catch (Exception e) {
            log.warn("Could not parse MBTA response: {}", e.getMessage());
            return positions;
        }

        for (JsonNode vehicle : data) {
            JsonNode attrs = vehicle.path("attributes");
            JsonNode rels = vehicle.path("relationships");
            String routeId = relationshipId(rels, "route");

            if (routeId == null || !attrs.hasNonNull("latitude") || !attrs.hasNonNull("longitude")
                    || !attrs.hasNonNull("updated_at")) {
                continue;
            }

            positions.add(new VehiclePosition(
                    vehicle.path("id").asText(),
                    attrs.path("label").asText(null),
                    routeId,
                    attrs.path("direction_id").asInt(0),
                    relationshipId(rels, "trip"),
                    relationshipId(rels, "stop"),
                    attrs.hasNonNull("current_stop_sequence") ? attrs.get("current_stop_sequence").asInt() : null,
                    attrs.path("current_status").asText(null),
                    attrs.get("latitude").asDouble(),
                    attrs.get("longitude").asDouble(),
                    OffsetDateTime.parse(attrs.get("updated_at").asText()).toInstant()));
        }
        return positions;
    }

    private static String relationshipId(JsonNode relationships, String name) {
        JsonNode id = relationships.path(name).path("data").path("id");
        return id.isMissingNode() || id.isNull() ? null : id.asText();
    }
}
