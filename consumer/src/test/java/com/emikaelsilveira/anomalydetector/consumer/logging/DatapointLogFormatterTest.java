package com.emikaelsilveira.anomalydetector.consumer.logging;

import java.time.Instant;
import java.util.Locale;
import java.util.OptionalDouble;
import java.util.UUID;

import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;
import com.emikaelsilveira.anomalydetector.consumer.detection.DetectionResult;
import com.emikaelsilveira.anomalydetector.consumer.detection.DetectionStatus;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DatapointLogFormatterTest {

    private final DatapointLogFormatter formatter = new DatapointLogFormatter();

    @Test
    void formatsTheExactR11OkLine() {
        assertThat(formatter.format(datapoint(100.42d, Instant.parse("2026-08-05T14:22:03.114Z")),
                result(DetectionStatus.OK, OptionalDouble.of(0.31d), 50, 30, 50)))
                .isEqualTo("[2026-08-05T14:22:03.114Z] Data point: 100.42 | Status: OK | Z-score: 0.31");
    }

    @Test
    void formatsTheExactR11AnomalyLine() {
        assertThat(formatter.format(datapoint(152.88d, Instant.parse("2026-08-05T14:22:07.372Z")),
                result(DetectionStatus.ANOMALY, OptionalDouble.of(9.84d), 50, 30, 50)))
                .isEqualTo("[2026-08-05T14:22:07.372Z] Data point: 152.88 | Status: ANOMALY DETECTED! | Z-score: 9.84 | ALERT: Significant deviation detected.");
    }

    @Test
    void formatsTheDocumentedWarmUpExceptionWithTheMinimumSampleDenominator() {
        assertThat(formatter.format(datapoint(99.87d, Instant.parse("2026-08-05T14:21:51.002Z")),
                result(DetectionStatus.WARMING_UP, OptionalDouble.empty(), 1, 30, 50)))
                .isEqualTo("[2026-08-05T14:21:51.002Z] Data point: 99.87 | Status: WARMING_UP | Samples: 1/30");
    }

    @Test
    void formatsTheDegenerateExceptionWithTheCapacityDenominator() {
        assertThat(formatter.format(datapoint(100.00d, Instant.parse("2026-08-05T14:21:51.002Z")),
                result(DetectionStatus.DEGENERATE_WINDOW, OptionalDouble.empty(), 31, 30, 50)))
                .isEqualTo("[2026-08-05T14:21:51.002Z] Data point: 100.00 | Status: DEGENERATE_WINDOW | Samples: 31/50");
    }

    @Test
    void usesTheRootLocaleAndNormalizesNegativeZero() {
        Locale previousLocale = Locale.getDefault();
        Locale.setDefault(Locale.forLanguageTag("pt-BR"));
        try {
            assertThat(formatter.format(datapoint(-0.0d, Instant.parse("2026-08-05T14:22:03.114Z")),
                    result(DetectionStatus.OK, OptionalDouble.of(-0.0d), 50, 30, 50)))
                    .isEqualTo("[2026-08-05T14:22:03.114Z] Data point: 0.00 | Status: OK | Z-score: 0.00");
        } finally {
            Locale.setDefault(previousLocale);
        }
    }

    @Test
    void rendersEventTimesInUtcWithMillisecondPrecision() {
        assertThat(formatter.format(datapoint(100.42d, Instant.parse("2026-08-05T16:22:03.114+02:00")),
                result(DetectionStatus.OK, OptionalDouble.of(0.31d), 50, 30, 50)))
                .startsWith("[2026-08-05T14:22:03.114Z]");
    }

    private DetectionResult result(
            DetectionStatus status,
            OptionalDouble zScore,
            int samples,
            int minSamples,
            int capacity
    ) {
        return new DetectionResult(status, zScore, samples, minSamples, capacity);
    }

    private Datapoint datapoint(double value, Instant emittedAt) {
        return new Datapoint(
                UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93"),
                1041L,
                "sensor.temperature",
                value,
                emittedAt
        );
    }
}
