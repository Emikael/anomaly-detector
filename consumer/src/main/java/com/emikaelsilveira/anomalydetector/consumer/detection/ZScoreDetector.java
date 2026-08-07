package com.emikaelsilveira.anomalydetector.consumer.detection;

import java.util.OptionalDouble;

/**
 * Leave-one-out rolling Z-score detector.
 *
 * <p>Each observation is scored against the window as it stands <em>before</em> the observation is
 * considered for admission. Including the point in its own reference caps the attainable score at
 * {@code (n-1)/sqrt(n)} — about 6.93 at N=50 — so the most extreme outliers would be the most
 * strongly suppressed. See ADR 0003.
 *
 * <p>Detected anomalies are withheld from the window so one outlier cannot inflate the variance and
 * mask the next. A run of {@code consecutiveOverride} anomalies is treated as a regime shift and
 * admitted in arrival order, which is what lets the reference follow a legitimate level shift
 * instead of alarming forever. See ADR 0004.
 *
 * <p>Stateful detector for serialized use only. It is framework-free, not referentially pure, and
 * not thread-safe.
 */
public final class ZScoreDetector {

    private static final double DEGENERATE_SIGMA_THRESHOLD = 1e-12;

    private final RollingWindow window;
    private final int capacity;
    private final int minSamples;
    private final double threshold;
    private final boolean excludeAnomalies;
    private final int consecutiveOverride;
    private final double[] pendingAnomalies;
    private int pendingCount;

    public ZScoreDetector(int windowSize, int minSamples, double threshold, boolean excludeAnomalies,
                          int consecutiveOverride) {
        validateConfiguration(windowSize, minSamples, threshold, consecutiveOverride);
        this.window = new RollingWindow(windowSize);
        this.capacity = windowSize;
        this.minSamples = minSamples;
        this.threshold = threshold;
        this.excludeAnomalies = excludeAnomalies;
        this.consecutiveOverride = consecutiveOverride;
        this.pendingAnomalies = new double[consecutiveOverride];
    }

    public DetectionResult evaluate(double value) {
        int referenceSamples = window.size();
        DetectionStatus status;
        OptionalDouble zScore;

        if (referenceSamples < minSamples) {
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

        admit(value, status);
        return new DetectionResult(status, zScore, referenceSamples, minSamples, capacity);
    }

    private void admit(double value, DetectionStatus status) {
        if (!excludeAnomalies) {
            window.add(value);
            return;
        }
        if (status != DetectionStatus.ANOMALY) {
            pendingCount = 0;
            window.add(value);
            return;
        }

        pendingAnomalies[pendingCount++] = value;
        if (pendingCount >= consecutiveOverride) {
            for (int index = 0; index < pendingCount; index++) {
                window.add(pendingAnomalies[index]);
            }
            pendingCount = 0;
        }
    }

    /**
     * Re-validates the bounds that {@code DetectorProperties} also enforces at boot. The duplication is
     * deliberate: this class is framework-free and constructible outside Spring, so it cannot rely on
     * bean validation having run. The bounds mirror the brief's N in 50..100.
     */
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
