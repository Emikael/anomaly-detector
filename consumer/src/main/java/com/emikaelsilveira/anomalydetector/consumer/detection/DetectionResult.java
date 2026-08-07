package com.emikaelsilveira.anomalydetector.consumer.detection;

import java.util.OptionalDouble;

/**
 * Verdict for a single observation.
 *
 * @param status           the verdict
 * @param zScore           the score; absent for {@link DetectionStatus#WARMING_UP} and
 *                         {@link DetectionStatus#DEGENERATE_WINDOW}, where no claim can be made
 * @param referenceSamples size of the rolling window the verdict was computed against, measured
 *                         <em>before</em> the observation itself was considered for admission
 * @param minSamples       warm-up floor the reference must reach before a verdict is produced
 * @param capacity         maximum rolling window size
 */
public record DetectionResult(
        DetectionStatus status,
        OptionalDouble zScore,
        int referenceSamples,
        int minSamples,
        int capacity
) {
}
