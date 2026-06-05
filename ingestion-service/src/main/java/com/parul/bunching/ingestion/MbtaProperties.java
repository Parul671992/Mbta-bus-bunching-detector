package com.parul.bunching.ingestion;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Settings for talking to the MBTA V3 API.
 *
 * @param baseUrl        API host
 * @param apiKey         optional key; anonymous use is limited to about 20 requests per minute
 * @param routes         bus routes to watch
 * @param pollIntervalMs delay between polls
 */
@ConfigurationProperties(prefix = "mbta")
public record MbtaProperties(String baseUrl, String apiKey, List<String> routes, long pollIntervalMs) {
}
