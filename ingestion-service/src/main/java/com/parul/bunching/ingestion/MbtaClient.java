package com.parul.bunching.ingestion;

import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClient;

/** Thin HTTP client for the MBTA V3 {@code /vehicles} endpoint. */
@Component
public class MbtaClient {

    private final RestClient restClient;
    private final MbtaProperties properties;

    public MbtaClient(RestClient.Builder builder, MbtaProperties properties) {
        this.properties = properties;
        RestClient.Builder configured = builder
                .baseUrl(properties.baseUrl())
                .defaultHeader("Accept", "application/vnd.api+json");
        if (StringUtils.hasText(properties.apiKey())) {
            configured.defaultHeader("x-api-key", properties.apiKey());
        }
        this.restClient = configured.build();
    }

    /** Fetches every vehicle currently serving the configured routes, in one request. */
    public String fetchVehicles() {
        return restClient.get()
                .uri(uri -> uri.path("/vehicles")
                        .queryParam("filter[route]", String.join(",", properties.routes()))
                        .build())
                .retrieve()
                .body(String.class);
    }
}
