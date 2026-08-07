package com.emikaelsilveira.anomalydetector.consumer.detection;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Gatherers;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.data.Offset.offset;

class ZScoreDetectorTest {

    @Test
    void ec01_belowMinimumSamplesWarmsUp() {
        ZScoreDetector detector = new ZScoreDetector(50, 30, 3.0, true, 5);

        DetectionResult result = null;
        for (int value = 1; value <= 30; value++) {
            result = detector.evaluate(value);
            assertThat(result.status()).isEqualTo(DetectionStatus.WARMING_UP);
            assertThat(result.zScore()).isEmpty();
            assertThat(result.referenceSamples()).isEqualTo(value - 1);
        }

        assertThat(result.minSamples()).isEqualTo(30);
        assertThat(result.capacity()).isEqualTo(50);
    }

    @Test
    void theSampleFloorIsReachedByTheReferenceWindowNotTheCurrentPoint() {
        ZScoreDetector detector = new ZScoreDetector(50, 30, 3.0, true, 5);
        for (int value = 1; value <= 29; value++) {
            detector.evaluate(value);
        }

        DetectionResult lastWarmUp = detector.evaluate(30.0);
        DetectionResult firstVerdict = detector.evaluate(31.0);

        // The reported count is the reference the verdict was computed against, so the final
        // warm-up line reads 29/30 rather than a self-contradictory 30/30.
        assertThat(lastWarmUp.status()).isEqualTo(DetectionStatus.WARMING_UP);
        assertThat(lastWarmUp.referenceSamples()).isEqualTo(29);
        assertThat(firstVerdict.status()).isEqualTo(DetectionStatus.OK);
        assertThat(firstVerdict.referenceSamples()).isEqualTo(30);
    }

    @Test
    void ec02_constantWindowIsDegenerate() {
        ZScoreDetector detector = new ZScoreDetector(50, 30, 3.0, true, 5);
        for (int index = 0; index < 30; index++) {
            detector.evaluate(10.0);
        }

        DetectionResult result = detector.evaluate(10.0);

        assertThat(result.status()).isEqualTo(DetectionStatus.DEGENERATE_WINDOW);
        assertThat(result.zScore()).isEmpty();
        assertThat(result.referenceSamples()).isEqualTo(30);
    }

    @Test
    void ec03_currentPointIsExcludedAndZAboveSevenIsReachable() {
        ZScoreDetector detector = detectorPrimedWithGoldenWindow(3.0);

        DetectionResult result = detector.evaluate(150.0);

        assertThat(result.status()).isEqualTo(DetectionStatus.ANOMALY);
        assertThat(result.zScore()).hasValue(8.54062954009694);
        assertThat(result.referenceSamples()).isEqualTo(50);
    }

    @Test
    void strictThresholdEqualityIsOk() {
        ZScoreDetector detector = detectorPrimedWithGoldenWindow(5.110657837246763);

        DetectionResult result = detector.evaluate(100.0);

        assertThat(result.status()).isEqualTo(DetectionStatus.OK);
        assertThat(result.zScore()).hasValue(5.110657837246763);
    }

    @Test
    void goldenSlidingWindowScoresAreExact() {
        // Hand-computed, not read back from the implementation. For n consecutive integers the sum of
        // squared deviations is n(n^2-1)/12 = 10412.5 at n=50, so the Bessel-corrected sigma is
        // sqrt(10412.5/49) for both ramps; the means are the midpoints 25.5 and 26.5.
        double sigma = Math.sqrt(10412.5d / 49.0d);
        List<List<Integer>> windows = IntStream.rangeClosed(1, 51)
                .boxed()
                .gather(Gatherers.windowSliding(50))
                .toList();

        assertThat(windows).hasSize(2);
        assertThat(scoreAgainst(windows.getFirst(), 100.0)).isCloseTo(74.5d / sigma, offset(1e-12));
        assertThat(scoreAgainst(windows.getFirst(), 150.0)).isCloseTo(124.5d / sigma, offset(1e-12));
        assertThat(scoreAgainst(windows.getLast(), 100.0)).isCloseTo(73.5d / sigma, offset(1e-12));
        assertThat(scoreAgainst(windows.getLast(), 150.0)).isCloseTo(123.5d / sigma, offset(1e-12));
    }

    @Test
    void ec10_negativeAndZeroValuesRemainValid() {
        ZScoreDetector detector = new ZScoreDetector(50, 2, 3.0, true, 5);

        detector.evaluate(-2.0);
        detector.evaluate(0.0);
        DetectionResult result = detector.evaluate(2.0);

        assertThat(result.status()).isEqualTo(DetectionStatus.OK);
        assertThat(result.zScore()).isPresent();
        assertThat(result.zScore().getAsDouble()).isCloseTo(2.1213203435596424, offset(1e-12));
    }

    @Test
    void ec05_twoOutliersThreePositionsApartAreBothCaught() {
        ZScoreDetector detector = detectorPrimedWithGoldenWindow(3.0);

        DetectionResult firstOutlier = detector.evaluate(100.0);
        detector.evaluate(25.0);
        detector.evaluate(26.0);
        detector.evaluate(27.0);
        DetectionResult secondOutlier = detector.evaluate(100.0);

        assertThat(firstOutlier.status()).isEqualTo(DetectionStatus.ANOMALY);
        assertThat(secondOutlier.status()).isEqualTo(DetectionStatus.ANOMALY);
    }

    @Test
    void aRunShorterThanTheOverrideIsDiscardedWhenANormalPointArrives() {
        ZScoreDetector detector = detectorWithTwoPointBaseline(true);

        for (int value = 100; value < 104; value++) {
            assertThat(detector.evaluate(value).status()).isEqualTo(DetectionStatus.ANOMALY);
        }
        DetectionResult normal = detector.evaluate(0.0);
        DetectionResult afterNormal = detector.evaluate(0.5);

        assertThat(normal.status()).isEqualTo(DetectionStatus.OK);
        assertThat(normal.referenceSamples()).isEqualTo(2);
        // Baseline pair plus the normal point only: the four buffered anomalies were discarded.
        assertThat(afterNormal.referenceSamples()).isEqualTo(3);
    }

    @Test
    void fifthConsecutiveAnomalyAdmitsTheBufferedRun() {
        ZScoreDetector detector = detectorWithTwoPointBaseline(true);

        for (int value = 100; value < 104; value++) {
            assertThat(detector.evaluate(value).status()).isEqualTo(DetectionStatus.ANOMALY);
        }
        DetectionResult fifthAnomaly = detector.evaluate(104.0);
        DetectionResult afterAdmission = detector.evaluate(104.5);

        assertThat(fifthAnomaly.status()).isEqualTo(DetectionStatus.ANOMALY);
        assertThat(fifthAnomaly.referenceSamples()).isEqualTo(2);
        // Baseline pair plus all five buffered anomalies, admitted in arrival order.
        assertThat(afterAdmission.referenceSamples()).isEqualTo(7);
    }

    @Test
    void disabledExclusionImmediatelyAdmitsAnomalies() {
        ZScoreDetector detector = detectorWithTwoPointBaseline(false);

        DetectionResult anomaly = detector.evaluate(100.0);
        DetectionResult next = detector.evaluate(101.0);

        assertThat(anomaly.status()).isEqualTo(DetectionStatus.ANOMALY);
        assertThat(anomaly.referenceSamples()).isEqualTo(2);
        assertThat(next.referenceSamples()).isEqualTo(3);
    }

    @Test
    void ec06_aLevelShiftAlarmsThenReBaselinesOnceTheOverrideAdmitsTheRun() {
        ZScoreDetector detector = new ZScoreDetector(50, 30, 3.0, true, 5);
        Random noise = new Random(7);
        for (int index = 0; index < 60; index++) {
            detector.evaluate(100.0d + noise.nextGaussian() * 5.0d);
        }

        List<DetectionStatus> afterShift = new ArrayList<>();
        for (int index = 0; index < 40; index++) {
            afterShift.add(detector.evaluate(150.0d + noise.nextGaussian() * 5.0d).status());
        }

        // Every point is anomalous against the frozen pre-shift reference until the fifth consecutive
        // anomaly admits the buffered run; the window then follows the new level instead of alarming
        // forever, which is the whole justification for the override (ADR 0004).
        assertThat(afterShift.subList(0, 5)).containsOnly(DetectionStatus.ANOMALY);
        assertThat(afterShift.subList(30, 40)).containsOnly(DetectionStatus.OK);
    }

    @Test
    void rejectsInvalidDetectorConfiguration() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ZScoreDetector(49, 2, 3.0, true, 2));
        assertThatIllegalArgumentException().isThrownBy(() -> new ZScoreDetector(101, 2, 3.0, true, 2));
        assertThatIllegalArgumentException().isThrownBy(() -> new ZScoreDetector(50, 1, 3.0, true, 2));
        assertThatIllegalArgumentException().isThrownBy(() -> new ZScoreDetector(50, 51, 3.0, true, 2));
        assertThatIllegalArgumentException().isThrownBy(() -> new ZScoreDetector(50, 2, 0.49, true, 2));
        assertThatIllegalArgumentException().isThrownBy(() -> new ZScoreDetector(50, 2, Double.NaN, true, 2));
        assertThatIllegalArgumentException().isThrownBy(() -> new ZScoreDetector(50, 2, 3.0, true, 1));
        assertThatIllegalArgumentException().isThrownBy(() -> new ZScoreDetector(50, 2, 3.0, true, 51));
    }

    private ZScoreDetector detectorWithTwoPointBaseline(boolean excludeAnomalies) {
        ZScoreDetector detector = new ZScoreDetector(50, 2, 3.0, excludeAnomalies, 5);
        detector.evaluate(0.0);
        detector.evaluate(1.0);
        return detector;
    }

    private double scoreAgainst(List<Integer> window, double candidate) {
        ZScoreDetector detector = new ZScoreDetector(50, 50, 3.0, true, 5);
        window.forEach(detector::evaluate);
        return detector.evaluate(candidate).zScore().orElseThrow();
    }

    private ZScoreDetector detectorPrimedWithGoldenWindow(double threshold) {
        ZScoreDetector detector = new ZScoreDetector(50, 50, threshold, true, 5);
        goldenWindows().getFirst().forEach(detector::evaluate);
        return detector;
    }

    private List<List<Integer>> goldenWindows() {
        return IntStream.rangeClosed(1, 51)
                .boxed()
                .gather(Gatherers.windowSliding(50))
                .toList();
    }
}
