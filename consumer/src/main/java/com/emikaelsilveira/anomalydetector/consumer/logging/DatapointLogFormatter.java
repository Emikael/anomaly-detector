package com.emikaelsilveira.anomalydetector.consumer.logging;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;
import java.util.Objects;

import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;
import com.emikaelsilveira.anomalydetector.consumer.detection.DetectionResult;

public final class DatapointLogFormatter {

    static final String ALERT = "Significant deviation detected.";

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendInstant(3)
            .toFormatter();

    public String format(Datapoint datapoint, DetectionResult result) {
        Objects.requireNonNull(datapoint, "datapoint");
        Objects.requireNonNull(result, "result");
        String emittedAt = formatTimestamp(datapoint.emittedAt());
        double value = normalizedZero(datapoint.value());

        return switch (result.status()) {
            case OK -> String.format(
                    Locale.ROOT,
                    "[%s] Data point: %.2f | Status: OK | Z-score: %.2f",
                    emittedAt,
                    value,
                    zScore(result)
            );
            case ANOMALY -> String.format(
                    Locale.ROOT,
                    "[%s] Data point: %.2f | Status: ANOMALY DETECTED! | Z-score: %.2f | ALERT: %s",
                    emittedAt,
                    value,
                    zScore(result),
                    ALERT
            );
            case WARMING_UP -> String.format(
                    Locale.ROOT,
                    "[%s] Data point: %.2f | Status: WARMING_UP | Samples: %d/%d",
                    emittedAt,
                    value,
                    result.referenceSamples(),
                    result.minSamples()
            );
            // The brief does not define this line. "Window" rather than "Samples" because the
            // denominator here is capacity, not the warm-up floor used above.
            case DEGENERATE_WINDOW -> String.format(
                    Locale.ROOT,
                    "[%s] Data point: %.2f | Status: DEGENERATE_WINDOW | Window: %d/%d",
                    emittedAt,
                    value,
                    result.referenceSamples(),
                    result.capacity()
            );
        };
    }

    String formatTimestamp(Instant instant) {
        return TIMESTAMP_FORMATTER.format(Objects.requireNonNull(instant, "emittedAt"));
    }

    Double structuredZScore(DetectionResult result) {
        return result.zScore().isPresent() ? zScore(result) : null;
    }

    double normalizedZero(double value) {
        return value == 0.0d ? 0.0d : value;
    }

    private double zScore(DetectionResult result) {
        double score = result.zScore().orElseThrow(() -> new IllegalArgumentException("zScore is required"));
        if (!Double.isFinite(score)) {
            throw new IllegalArgumentException("zScore must be finite");
        }
        return normalizedZero(score);
    }
}
