package com.emikaelsilveira.anomalydetector.consumer.detection;

import java.util.OptionalDouble;

public record DetectionResult(
        DetectionStatus status,
        OptionalDouble zScore,
        int samples,
        int minSamples,
        int capacity
) {
}
