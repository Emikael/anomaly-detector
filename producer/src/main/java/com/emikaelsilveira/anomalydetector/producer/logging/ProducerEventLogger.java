package com.emikaelsilveira.anomalydetector.producer.logging;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;

import com.emikaelsilveira.anomalydetector.producer.contract.Datapoint;
import com.emikaelsilveira.anomalydetector.producer.generation.AnomalyInjection;
import com.emikaelsilveira.anomalydetector.producer.generation.GeneratedDatapoint;
import com.emikaelsilveira.anomalydetector.producer.generation.LevelShift;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;

/**
 * Takes its {@link Logger} by injection rather than declaring a static one, so tests can attach an
 * appender and assert on the exact rendered line without touching global logging state.
 */
@RequiredArgsConstructor
public final class ProducerEventLogger {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendInstant(3)
            .toFormatter();

    private final @NonNull Logger logger;

    public void log(GeneratedDatapoint generatedDatapoint) {
        Datapoint datapoint = generatedDatapoint.datapoint();
        generatedDatapoint.anomalyInjection().ifPresentOrElse(
                anomalyInjection -> logInjectedAnomaly(datapoint, anomalyInjection),
                () -> logDatapoint(datapoint)
        );
        generatedDatapoint.levelShift().ifPresent(levelShift -> logLevelShift(datapoint.emittedAt(), levelShift));
    }

    private void logDatapoint(Datapoint datapoint) {
        String emittedAt = formatTimestamp(datapoint.emittedAt());
        logger.atDebug()
                .addKeyValue("event", "datapoint")
                .addKeyValue("emittedAt", emittedAt)
                .addKeyValue("sequence", datapoint.sequence())
                .addKeyValue("metric", datapoint.metric())
                .addKeyValue("value", normalizedZero(datapoint.value()))
                .log(String.format(
                        Locale.ROOT,
                        "[%s] DATAPOINT seq=%d value=%.2f",
                        emittedAt,
                        datapoint.sequence(),
                        normalizedZero(datapoint.value())
                ));
    }

    private void logInjectedAnomaly(Datapoint datapoint, AnomalyInjection anomalyInjection) {
        String emittedAt = formatTimestamp(datapoint.emittedAt());
        double signedSigma = normalizedZero(anomalyInjection.signedSigma());
        logger.atInfo()
                .addKeyValue("event", "injected_anomaly")
                .addKeyValue("emittedAt", emittedAt)
                .addKeyValue("sequence", datapoint.sequence())
                .addKeyValue("metric", datapoint.metric())
                .addKeyValue("value", normalizedZero(datapoint.value()))
                .addKeyValue("sigma", signedSigma)
                .log(String.format(
                        Locale.ROOT,
                        "[%s] INJECTED ANOMALY seq=%d value=%.2f (%+.1fσ)",
                        emittedAt,
                        datapoint.sequence(),
                        normalizedZero(datapoint.value()),
                        signedSigma
                ));
    }

    private void logLevelShift(Instant emittedAt, LevelShift levelShift) {
        String timestamp = formatTimestamp(emittedAt);
        double sigma = normalizedZero(levelShift.sigma());
        logger.atInfo()
                .addKeyValue("event", "level_shift")
                .addKeyValue("emittedAt", timestamp)
                .addKeyValue("sequence", levelShift.sequence())
                .addKeyValue("oldMean", normalizedZero(levelShift.oldMean()))
                .addKeyValue("newMean", normalizedZero(levelShift.newMean()))
                .addKeyValue("sigma", sigma)
                .log(String.format(
                        Locale.ROOT,
                        "[%s] *** LEVEL SHIFT at seq=%d: mean %.2f -> %.2f (%+.1fσ) ***",
                        timestamp,
                        levelShift.sequence(),
                        normalizedZero(levelShift.oldMean()),
                        normalizedZero(levelShift.newMean()),
                        sigma
                ));
    }

    private String formatTimestamp(Instant instant) {
        return TIMESTAMP_FORMATTER.format(instant);
    }

    private double normalizedZero(double value) {
        return value == 0.0 ? 0.0 : value;
    }
}
