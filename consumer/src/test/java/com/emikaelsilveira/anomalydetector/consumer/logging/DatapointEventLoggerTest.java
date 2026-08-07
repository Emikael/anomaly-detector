package com.emikaelsilveira.anomalydetector.consumer.logging;

import java.time.Instant;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.UUID;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;
import com.emikaelsilveira.anomalydetector.consumer.detection.DetectionResult;
import com.emikaelsilveira.anomalydetector.consumer.detection.DetectionStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

class DatapointEventLoggerTest {

    private Logger logger;
    private ListAppender<ILoggingEvent> appender;
    private Level previousLevel;

    @BeforeEach
    void captureLogs() {
        logger = (Logger) LoggerFactory.getLogger(DatapointEventLogger.class);
        previousLevel = logger.getLevel();
        logger.setLevel(Level.INFO);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void stopCapturingLogs() {
        logger.detachAppender(appender);
        appender.stop();
        logger.setLevel(previousLevel);
    }

    @Test
    void logsAnomalyAtInfoWithStableStructuredKeysAndAlert() {
        new DatapointEventLogger(logger, new DatapointLogFormatter()).log(
                datapoint(152.88d),
                result(DetectionStatus.ANOMALY, OptionalDouble.of(9.84d), 50, 30, 50)
        );

        assertThat(appender.list).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.INFO);
            assertThat(event.getFormattedMessage()).isEqualTo(
                    "[2026-08-05T14:22:07.372Z] Data point: 152.88 | Status: ANOMALY DETECTED! | Z-score: 9.84 | ALERT: Significant deviation detected.");
            assertThat(keyValues(event)).containsExactlyInAnyOrderEntriesOf(Map.of(
                    "event", "detection",
                    "emittedAt", "2026-08-05T14:22:07.372Z",
                    "sequence", 1041L,
                    "metric", "sensor.temperature",
                    "value", 152.88d,
                    "status", "ANOMALY",
                    "zScore", 9.84d,
                    "referenceSamples", 50,
                    "capacity", 50,
                    "alert", "Significant deviation detected."
            ));
        });
    }

    @Test
    void omitsTheStructuredAlertForNonAnomalies() {
        new DatapointEventLogger(logger, new DatapointLogFormatter()).log(
                datapoint(100.42d),
                result(DetectionStatus.OK, OptionalDouble.of(0.31d), 50, 30, 50)
        );

        assertThat(keyValues(appender.list.getFirst())).doesNotContainKey("alert");
    }

    private Map<String, Object> keyValues(ILoggingEvent event) {
        return event.getKeyValuePairs().stream().collect(Collectors.toMap(pair -> pair.key, pair -> pair.value));
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

    private Datapoint datapoint(double value) {
        return new Datapoint(
                UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93"),
                1041L,
                "sensor.temperature",
                value,
                Instant.parse("2026-08-05T14:22:07.372Z")
        );
    }
}
