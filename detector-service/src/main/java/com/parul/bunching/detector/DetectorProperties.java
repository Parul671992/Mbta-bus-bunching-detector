package com.parul.bunching.detector;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "detector")
public record DetectorProperties(
        double thresholdMeters,
        long staleAfterSeconds,
        long cooldownMinutes,
        boolean replayFromBeginning) {

    public Duration staleAfter() {
        return Duration.ofSeconds(staleAfterSeconds);
    }

    public Duration cooldown() {
        return Duration.ofMinutes(cooldownMinutes);
    }
}
