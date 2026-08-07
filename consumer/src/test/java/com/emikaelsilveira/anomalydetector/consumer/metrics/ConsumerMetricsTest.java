package com.emikaelsilveira.anomalydetector.consumer.metrics;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.OptionalDouble;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.emikaelsilveira.anomalydetector.consumer.detection.DetectionResult;
import com.emikaelsilveira.anomalydetector.consumer.detection.DetectionStatus;
import com.emikaelsilveira.anomalydetector.consumer.logging.ConsoleEventLog;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumerMetricsTest {

    private static final Clock PROCESSING_CLOCK = Clock.fixed(
            Instant.parse("2026-08-05T14:22:07.361Z"),
            ZoneOffset.UTC
    );

    private SimpleMeterRegistry registry;
    private Logger logger;
    private ListAppender<ILoggingEvent> appender;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        logger = (Logger) LoggerFactory.getLogger(ConsoleEventLog.NAME);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(appender);
        appender.stop();
        registry.close();
    }

    @Test
    void registersExactMeterNamesAndRecordsSuccessfulProcessing() {
        ConsumerMetrics metrics = metrics(10);

        metrics.recordProcessed(result(DetectionStatus.WARMING_UP, OptionalDouble.empty(), 1), micros(12));

        assertThat(registry.get("anomaly.detector.points.processed").counter().count()).isEqualTo(1.0d);
        assertThat(registry.get("anomaly.detector.points.anomalies").counter().count()).isZero();
        assertThat(registry.get("anomaly.detector.points.rejected").counter().count()).isZero();
        assertThat(registry.get("anomaly.detector.points.duplicates").counter().count()).isZero();
        assertThat(registry.get("anomaly.detector.window.occupancy").gauge().value()).isEqualTo(1.0d);
        assertThat(registry.get("anomaly.detector.zscore").summary().count()).isZero();
        assertThat(registry.get("anomaly.detector.processing").timer().count()).isEqualTo(1L);
    }

    @Test
    void incrementsAnomalyDuplicateAndRejectedCountersWithoutChangingSuccessfulCadence() {
        ConsumerMetrics metrics = metrics(2);

        metrics.recordDuplicate();
        metrics.recordRejected();
        metrics.recordProcessed(result(DetectionStatus.ANOMALY, OptionalDouble.of(4.0d), 31), micros(20));

        assertThat(registry.get("anomaly.detector.points.anomalies").counter().count()).isEqualTo(1.0d);
        assertThat(registry.get("anomaly.detector.points.duplicates").counter().count()).isEqualTo(1.0d);
        assertThat(registry.get("anomaly.detector.points.rejected").counter().count()).isEqualTo(1.0d);
        assertThat(appender.list).isEmpty();
    }

    @Test
    void recordsOnlyFiniteZScoresInTheDistributionAndMean() {
        ConsumerMetrics metrics = metrics(10);

        metrics.recordProcessed(result(DetectionStatus.OK, OptionalDouble.of(Double.NaN), 30), Duration.ZERO);
        metrics.recordProcessed(result(DetectionStatus.OK, OptionalDouble.of(2.5d), 31), Duration.ZERO);

        assertThat(registry.get("anomaly.detector.zscore").summary().count()).isEqualTo(1L);
        assertThat(registry.get("anomaly.detector.zscore").summary().totalAmount()).isEqualTo(2.5d);
        assertThat(metrics.meanZScore()).isEqualTo(2.5d);
    }

    @Test
    void emitsSummariesOnlyOnSuccessfulNonDuplicateCadence() {
        ConsumerMetrics metrics = metrics(2);

        metrics.recordProcessed(result(DetectionStatus.OK, OptionalDouble.of(1.0d), 30), Duration.ZERO);
        metrics.recordDuplicate();
        metrics.recordRejected();
        assertThat(appender.list).isEmpty();

        metrics.recordProcessed(result(DetectionStatus.OK, OptionalDouble.of(3.0d), 31), Duration.ZERO);

        assertThat(appender.list).hasSize(1);
    }

    @Test
    void formatsCumulativeSummaryWithPercentageFiniteMeanAndIntegerP99Micros() {
        ConsumerMetrics metrics = metrics(2);

        metrics.recordDuplicate();
        metrics.recordRejected();
        metrics.recordProcessed(result(DetectionStatus.OK, OptionalDouble.of(2.0d), 30), micros(100));
        metrics.recordProcessed(result(DetectionStatus.ANOMALY, OptionalDouble.of(4.0d), 31), micros(300));

        assertThat(appender.list).singleElement().extracting(ILoggingEvent::getFormattedMessage)
                .asString()
                .matches("\\[2026-08-05T14:22:07.361Z] SUMMARY \\| processed=2 anomalies=1 \\(50\\.00%\\) rejected=1 duplicates=1 window=31/50 meanZ=3\\.00 p99ProcessingMicros=\\d+");
    }

    private ConsumerMetrics metrics(int summaryEvery) {
        return new ConsumerMetrics(registry, PROCESSING_CLOCK, summaryEvery, 50);
    }

    private Duration micros(long micros) {
        return Duration.ofNanos(micros * 1_000L);
    }

    private DetectionResult result(DetectionStatus status, OptionalDouble zScore, int samples) {
        return new DetectionResult(status, zScore, samples, 30, 50);
    }
}
