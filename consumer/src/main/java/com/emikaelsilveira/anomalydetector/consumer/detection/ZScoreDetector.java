package com.emikaelsilveira.anomalydetector.consumer.detection;

import java.util.OptionalDouble;

/**
 * Stateful detector for serialized use only. It is framework-free, not referentially pure, and not thread-safe.
 */
public final class ZScoreDetector {

    private static final double DEGENERATE_SIGMA_THRESHOLD = 1e-12;

    private final RollingWindow window;
    private final int capacity;
    private final int minSamples;
    private final double threshold;
    private final boolean excludeAnomalies;
    private final int consecutiveOverride;

    public ZScoreDetector(int windowSize, int minSamples, double threshold, boolean excludeAnomalies,
                          int consecutiveOverride) {
        validateConfiguration(windowSize, minSamples, threshold, consecutiveOverride);
        this.window = new RollingWindow(windowSize);
        this.capacity = windowSize;
        this.minSamples = minSamples;
        this.threshold = threshold;
        this.excludeAnomalies = excludeAnomalies;
        this.consecutiveOverride = consecutiveOverride;
    }

    public DetectionResult evaluate(double value) {
        DetectionStatus status;
        OptionalDouble zScore;

        if (window.size() < minSamples) {
            status = DetectionStatus.WARMING_UP;
            zScore = OptionalDouble.empty();
        } else {
            double mean = window.mean();
            double sigma = window.sampleStandardDeviation(mean);
            if (sigma < DEGENERATE_SIGMA_THRESHOLD) {
                status = DetectionStatus.DEGENERATE_WINDOW;
                zScore = OptionalDouble.empty();
            } else {
                double score = Math.abs(value - mean) / sigma;
                status = score > threshold ? DetectionStatus.ANOMALY : DetectionStatus.OK;
                zScore = OptionalDouble.of(score);
            }
        }

        window.add(value);
        return new DetectionResult(status, zScore, window.size(), minSamples, capacity);
    }


    private static void validateConfiguration(int windowSize, int minSamples, double threshold,
                                              int consecutiveOverride) {
        if (windowSize < 50 || windowSize > 100) {
            throw new IllegalArgumentException("windowSize must be between 50 and 100");
        }
        if (minSamples < 2 || minSamples > windowSize) {
            throw new IllegalArgumentException("minSamples must be between 2 and windowSize");
        }
        if (!Double.isFinite(threshold) || threshold < 0.5) {
            throw new IllegalArgumentException("threshold must be finite and at least 0.5");
        }
        if (consecutiveOverride < 2 || consecutiveOverride > windowSize) {
            throw new IllegalArgumentException("consecutiveOverride must be between 2 and windowSize");
        }
    }
}
