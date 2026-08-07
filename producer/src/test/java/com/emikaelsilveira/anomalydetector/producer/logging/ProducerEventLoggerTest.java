package com.emikaelsilveira.anomalydetector.producer.logging;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.emikaelsilveira.anomalydetector.producer.contract.Datapoint;
import com.emikaelsilveira.anomalydetector.producer.generation.AnomalyInjection;
import com.emikaelsilveira.anomalydetector.producer.generation.GeneratedDatapoint;
import com.emikaelsilveira.anomalydetector.producer.generation.LevelShift;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

class ProducerEventLoggerTest {

    private Logger logger;
    private ListAppender<ILoggingEvent> appender;
    private Level previousLevel;

    @BeforeEach
    void captureLogs() {
        logger = (Logger) LoggerFactory.getLogger(ProducerEventLogger.class);
        previousLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
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
    void logsTheRequiredInjectedAnomalyLiteralAtInfo() {
        ProducerEventLogger eventLogger = new ProducerEventLogger(logger);

        eventLogger.log(generated(152.88, Optional.of(new AnomalyInjection(10.6)), Optional.empty()));

        assertThat(appender.list).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.INFO);
            assertThat(event.getFormattedMessage()).isEqualTo(
                    "[2026-08-05T14:22:07.361Z] INJECTED ANOMALY seq=1041 value=152.88 (+10.6σ)");
        });
    }

    @Test
    void logsTheRequiredLevelShiftLiteralOnceAtInfo() {
        ProducerEventLogger eventLogger = new ProducerEventLogger(logger);
        LevelShift shift = new LevelShift(1041, 100.0, 150.0, 10.0);

        eventLogger.log(generated(100.0, Optional.empty(), Optional.of(shift)));

        assertThat(appender.list).filteredOn(event -> event.getLevel() == Level.INFO).singleElement().satisfies(event ->
                assertThat(event.getFormattedMessage()).isEqualTo(
                        "[2026-08-05T14:22:07.361Z] *** LEVEL SHIFT at seq=1041: mean 100.00 -> 150.00 (+10.0σ) ***"));
    }

    @Test
    void logsOrdinaryDatapointsAtDebugWithRootLocaleAndNormalizedNegativeZero() {
        Locale previousLocale = Locale.getDefault();
        Locale.setDefault(Locale.forLanguageTag("pt-BR"));
        try {
            new ProducerEventLogger(logger).log(generated(-0.0, Optional.empty(), Optional.empty()));
        } finally {
            Locale.setDefault(previousLocale);
        }

        assertThat(appender.list).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.DEBUG);
            assertThat(event.getFormattedMessage()).isEqualTo(
                    "[2026-08-05T14:22:07.361Z] DATAPOINT seq=1041 value=0.00");
        });
    }

    @Test
    void addsStableStructuredKeysToAnInjectedAnomaly() {
        new ProducerEventLogger(logger).log(generated(152.88, Optional.of(new AnomalyInjection(10.6)), Optional.empty()));

        Map<String, Object> keyValues = appender.list.getFirst().getKeyValuePairs().stream()
                .collect(Collectors.toMap(pair -> pair.key, pair -> pair.value));

        assertThat(keyValues).containsEntry("event", "injected_anomaly");
        assertThat(keyValues).containsEntry("emittedAt", "2026-08-05T14:22:07.361Z");
        assertThat(keyValues).containsEntry("sequence", 1041L);
        assertThat(keyValues).containsEntry("metric", "sensor.temperature");
        assertThat(keyValues).containsEntry("value", 152.88);
        assertThat(keyValues).containsEntry("sigma", 10.6);
    }

    private GeneratedDatapoint generated(
            double value,
            Optional<AnomalyInjection> anomalyInjection,
            Optional<LevelShift> levelShift
    ) {
        return new GeneratedDatapoint(
                new Datapoint(
                        UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93"),
                        1041,
                        "sensor.temperature",
                        value,
                        Instant.parse("2026-08-05T14:22:07.361Z")
                ),
                anomalyInjection,
                levelShift
        );
    }
}
