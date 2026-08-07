package com.emikaelsilveira.anomalydetector.consumer.detection;

import java.util.List;
import java.util.Random;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.DoubleRange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

class ZScoreDetectorProperties {

    @Property(tries = 100, seed = "894372")
    void affineTransformationsPreserveVerdicts(
            @ForAll("finiteBaseVectors") List<Double> values,
            @ForAll @DoubleRange(min = 0.1, max = 10.0) double scale,
            @ForAll @DoubleRange(min = -1_000_000.0, max = 1_000_000.0) double shift
    ) {
        ZScoreDetector original = new ZScoreDetector(50, 50, 3.0, false, 5);
        ZScoreDetector transformed = new ZScoreDetector(50, 50, 3.0, false, 5);

        for (double value : values) {
            DetectionResult originalResult = original.evaluate(value);
            DetectionResult transformedResult = transformed.evaluate(scale * value + shift);

            assertThat(transformedResult.status()).isEqualTo(originalResult.status());
            assertThat(transformedResult.zScore().isPresent()).isEqualTo(originalResult.zScore().isPresent());
            if (originalResult.zScore().isPresent()) {
                assertThat(transformedResult.zScore().getAsDouble())
                        .isCloseTo(originalResult.zScore().getAsDouble(), offset(1e-8));
            }
        }
    }

    @Property(tries = 100, seed = "234987")
    void occupancyNeverExceedsCapacity(
            @ForAll("finiteSequences") List<Double> values,
            @ForAll boolean excludeAnomalies
    ) {
        ZScoreDetector detector = new ZScoreDetector(50, 2, 3.0, excludeAnomalies, 5);

        for (double value : values) {
            assertThat(detector.evaluate(value).samples()).isLessThanOrEqualTo(50);
        }
    }

    @Property(tries = 1, seed = "42")
    void fixedSeedPureGaussianAnomalyRateStaysBelowOnePercent() {
        ZScoreDetector detector = new ZScoreDetector(50, 50, 3.0, false, 5);
        Random random = new Random(42);
        int anomalies = 0;
        int scored = 0;

        for (int index = 0; index < 10_000; index++) {
            DetectionResult result = detector.evaluate(random.nextGaussian());
            if (result.zScore().isPresent()) {
                scored++;
                if (result.status() == DetectionStatus.ANOMALY) {
                    anomalies++;
                }
            }
        }

        assertThat((double) anomalies / scored).isLessThan(0.01);
    }

    @Provide
    Arbitrary<List<Double>> finiteBaseVectors() {
        return Arbitraries.doubles()
                .between(-1_000_000.0, 1_000_000.0)
                .list()
                .ofMinSize(51)
                .ofMaxSize(100)
                .filter(this::everyRollingWindowHasSufficientDeviation);
    }

    @Provide
    Arbitrary<List<Double>> finiteSequences() {
        return Arbitraries.doubles()
                .between(-1_000_000.0, 1_000_000.0)
                .list()
                .ofMinSize(1)
                .ofMaxSize(300);
    }

    private boolean everyRollingWindowHasSufficientDeviation(List<Double> values) {
        for (int start = 0; start <= values.size() - 50; start++) {
            if (sampleStandardDeviation(values.subList(start, start + 50)) < 1e-6) {
                return false;
            }
        }
        return true;
    }

    private double sampleStandardDeviation(List<Double> values) {
        double total = 0.0;
        for (double value : values) {
            total += value;
        }
        double mean = total / values.size();
        double squaredDeviationTotal = 0.0;
        for (double value : values) {
            double deviation = value - mean;
            squaredDeviationTotal += deviation * deviation;
        }
        return Math.sqrt(squaredDeviationTotal / (values.size() - 1));
    }
}
