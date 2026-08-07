package com.emikaelsilveira.anomalydetector.consumer.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "detector")
@Validated
public record DetectorProperties(
        @Min(50) @Max(100) int windowSize,
        @DecimalMin("0.5") double zThreshold,
        @Min(2) int minSamples,
        boolean excludeAnomalies,
        @Min(2) int consecutiveOverride,
        @Min(1) int summaryEvery
) {

    @AssertTrue(message = "zThreshold must be finite")
    public boolean isZThresholdFinite() {
        return Double.isFinite(zThreshold);
    }

    @AssertTrue(message = "minSamples must not exceed windowSize")
    public boolean isMinSamplesWithinWindowSize() {
        return minSamples <= windowSize;
    }

    @AssertTrue(message = "consecutiveOverride must not exceed windowSize")
    public boolean isConsecutiveOverrideWithinWindowSize() {
        return consecutiveOverride <= windowSize;
    }
}
