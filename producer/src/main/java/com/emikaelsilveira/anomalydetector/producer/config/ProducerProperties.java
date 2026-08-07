package com.emikaelsilveira.anomalydetector.producer.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "producer")
@Validated
public record ProducerProperties(
        @Min(1) long intervalMs,
        double mean,
        @DecimalMin(value = "0.0", inclusive = false) double stddev,
        @DecimalMin("0.0") @DecimalMax("1.0") double anomalyProbability,
        @DecimalMin(value = "0.0", inclusive = false) double anomalySigmaMin,
        @DecimalMin(value = "0.0", inclusive = false) double anomalySigmaMax,
        Long seed,
        boolean levelShiftEnabled,
        @Min(1) long levelShiftAtSequence,
        @DecimalMin("0.0") double levelShiftSigma
) {

    @AssertTrue(message = "mean must be finite")
    public boolean isMeanFinite() {
        return Double.isFinite(mean);
    }

    @AssertTrue(message = "stddev must be finite")
    public boolean isStddevFinite() {
        return Double.isFinite(stddev);
    }

    @AssertTrue(message = "anomalyProbability must be finite")
    public boolean isAnomalyProbabilityFinite() {
        return Double.isFinite(anomalyProbability);
    }

    @AssertTrue(message = "anomalySigmaMin must be finite")
    public boolean isAnomalySigmaMinFinite() {
        return Double.isFinite(anomalySigmaMin);
    }

    @AssertTrue(message = "anomalySigmaMax must be finite")
    public boolean isAnomalySigmaMaxFinite() {
        return Double.isFinite(anomalySigmaMax);
    }

    @AssertTrue(message = "levelShiftSigma must be finite")
    public boolean isLevelShiftSigmaFinite() {
        return Double.isFinite(levelShiftSigma);
    }

    @AssertTrue(message = "anomalySigmaMin must not exceed anomalySigmaMax")
    public boolean isAnomalySigmaRangeValid() {
        return anomalySigmaMin <= anomalySigmaMax;
    }

    @AssertTrue(message = "mean, levelShiftSigma, anomalySigmaMax, and stddev must keep generated values within +/-1e150")
    public boolean isGeneratedValueWithinSafeRange() {
        if (!Double.isFinite(mean) || !Double.isFinite(stddev)
                || !Double.isFinite(anomalySigmaMax) || !Double.isFinite(levelShiftSigma)) {
            return false;
        }
        return Math.abs(mean) + (levelShiftSigma + anomalySigmaMax) * stddev <= 1.0e150;
    }
}
