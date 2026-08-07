package com.emikaelsilveira.anomalydetector.consumer.detection;

import java.util.List;
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
            assertThat(result.samples()).isEqualTo(value);
        }

        assertThat(result.minSamples()).isEqualTo(30);
        assertThat(result.capacity()).isEqualTo(50);
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
        assertThat(result.samples()).isEqualTo(31);
    }

    @Test
    void ec03_currentPointIsExcludedAndZAboveSevenIsReachable() {
        ZScoreDetector detector = detectorPrimedWithGoldenWindow(3.0);

        DetectionResult result = detector.evaluate(150.0);

        assertThat(result.status()).isEqualTo(DetectionStatus.ANOMALY);
        assertThat(result.zScore()).hasValue(8.54062954009694);
        assertThat(result.samples()).isEqualTo(50);
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
        List<List<Integer>> windows = IntStream.rangeClosed(1, 51)
                .boxed()
                .gather(Gatherers.windowSliding(50))
                .toList();

        assertThat(windows).hasSize(2);
        assertThat(windows.getFirst()).containsExactlyElementsOf(IntStream.rangeClosed(1, 50).boxed().toList());
        assertThat(windows.getLast()).containsExactlyElementsOf(IntStream.rangeClosed(2, 51).boxed().toList());

        DetectionResult result = detectorPrimedWithGoldenWindow(3.0).evaluate(100.0);
        assertThat(result.zScore()).hasValue(5.110657837246763);
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
    void fourAnomaliesThenNormalDoNotContaminateTheWindow() {
        ZScoreDetector detector = detectorWithTwoPointBaseline(true);

        for (int value = 100; value < 104; value++) {
            assertThat(detector.evaluate(value).status()).isEqualTo(DetectionStatus.ANOMALY);
        }
        DetectionResult normal = detector.evaluate(0.0);

        assertThat(normal.status()).isEqualTo(DetectionStatus.OK);
        assertThat(normal.samples()).isEqualTo(3);
    }

    @Test
    void fifthConsecutiveAnomalyAdmitsTheBufferedRun() {
        ZScoreDetector detector = detectorWithTwoPointBaseline(true);

        for (int value = 100; value < 104; value++) {
            assertThat(detector.evaluate(value).status()).isEqualTo(DetectionStatus.ANOMALY);
        }
        DetectionResult fifthAnomaly = detector.evaluate(104.0);

        assertThat(fifthAnomaly.status()).isEqualTo(DetectionStatus.ANOMALY);
        assertThat(fifthAnomaly.samples()).isEqualTo(7);
    }

    @Test
    void disabledExclusionImmediatelyAdmitsAnomalies() {
        ZScoreDetector detector = detectorWithTwoPointBaseline(false);

        DetectionResult anomaly = detector.evaluate(100.0);

        assertThat(anomaly.status()).isEqualTo(DetectionStatus.ANOMALY);
        assertThat(anomaly.samples()).isEqualTo(3);
    }

    @Test
    void ec06_sustainedShiftReturnsToOkWithinFiftyPoints() {
        ZScoreDetector detector = detectorPrimedWithGoldenWindow(3.0);

        for (int index = 0; index < 5; index++) {
            assertThat(detector.evaluate(100.0).status()).isEqualTo(DetectionStatus.ANOMALY);
        }
        DetectionResult rebaselined = detector.evaluate(100.0);

        assertThat(rebaselined.status()).isEqualTo(DetectionStatus.OK);
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
