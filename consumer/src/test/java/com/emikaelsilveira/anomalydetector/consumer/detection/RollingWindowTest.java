package com.emikaelsilveira.anomalydetector.consumer.detection;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

class RollingWindowTest {

    @Test
    void retainsArrivalOrderByEvictingTheOldestValueAtCapacity() {
        RollingWindow window = new RollingWindow(3);

        window.add(1.0);
        window.add(2.0);
        window.add(3.0);
        window.add(4.0);

        assertThat(window.size()).isEqualTo(3);
        assertThat(window.mean()).isEqualTo(3.0);
        assertThat(window.sampleStandardDeviation()).isCloseTo(1.0, offset(1e-12));
    }

    @Test
    void computesMeanWithAFullPassOverTheCurrentValues() {
        RollingWindow window = new RollingWindow(5);

        window.add(-4.0);
        window.add(2.0);
        window.add(8.0);

        assertThat(window.mean()).isCloseTo(2.0, offset(1e-12));
    }

    @Test
    void ec04_populationVsSampleSigmaUsesBesselCorrection() {
        RollingWindow window = new RollingWindow(50);
        for (int value = 1; value <= 50; value++) {
            window.add(value);
        }

        assertThat(window.mean()).isCloseTo(25.5, offset(1e-12));
        assertThat(window.sampleStandardDeviation()).isCloseTo(14.577379737113251, offset(1e-12));
    }

    @Test
    void ec07_recomputationDoesNotDriftOrProduceNaN() {
        RollingWindow window = new RollingWindow(100);
        for (int index = 0; index < 100; index++) {
            window.add(index % 2 == 0 ? 1e150 : -1e150);
        }

        assertThat(window.mean()).isFinite();
        assertThat(window.sampleStandardDeviation()).isFinite();
        assertThat(window.sampleStandardDeviation()).isGreaterThan(0.0);
    }
}
