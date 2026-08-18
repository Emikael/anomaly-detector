package com.emikaelsilveira.anomalydetector.producer.config;

import com.emikaelsilveira.anomalydetector.producer.generation.GenerationProfile;
import com.emikaelsilveira.anomalydetector.producer.generation.GenerationProfile.AnomalyProfile;
import com.emikaelsilveira.anomalydetector.producer.generation.GenerationProfile.LevelShiftProfile;
import com.emikaelsilveira.anomalydetector.producer.generation.GenerationProfile.NoiseProfile;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/** Binds and validates the synthetic signal, anomaly injection, and level-shift settings. */
@ConfigurationProperties(prefix = "producer")
@Validated
public record ProducerProperties(
        @Min(1) long intervalMs,
        @Finite(message = "mean must be finite") double mean,
        @Finite(message = "stddev must be finite")
        @DecimalMin(value = "0.0", inclusive = false) double stddev,
        @Finite(message = "anomalyProbability must be finite")
        @DecimalMin("0.0") @DecimalMax("1.0") double anomalyProbability,
        @Finite(message = "anomalySigmaMin must be finite")
        @DecimalMin(value = "0.0", inclusive = false) double anomalySigmaMin,
        @Finite(message = "anomalySigmaMax must be finite")
        @DecimalMin(value = "0.0", inclusive = false) double anomalySigmaMax,
        Long seed,
        boolean levelShiftEnabled,
        @Min(1) long levelShiftAtSequence,
        @Finite(message = "levelShiftSigma must be finite")
        @DecimalMin("0.0") double levelShiftSigma
) {

    /**
     * Assembles the generator's configuration here rather than in the composition root, which would
     * otherwise reach through eight accessors to build an object this record already has all the
     * data for. Only the validated values ever reach a generator.
     */
    public GenerationProfile generationProfile() {
        return new GenerationProfile(
                new NoiseProfile(mean, stddev),
                new AnomalyProfile(anomalyProbability, anomalySigmaMin, anomalySigmaMax),
                new LevelShiftProfile(levelShiftEnabled, levelShiftAtSequence, levelShiftSigma)
        );
    }

    // Only cross-field rules remain as @AssertTrue: each reads two or more components at once and so
    // belongs to the record rather than to any single one of them.

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
