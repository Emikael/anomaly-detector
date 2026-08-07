package com.emikaelsilveira.anomalydetector.consumer.processing;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;
import com.emikaelsilveira.anomalydetector.consumer.detection.ZScoreDetector;
import com.emikaelsilveira.anomalydetector.consumer.logging.DatapointEventLogger;
import com.emikaelsilveira.anomalydetector.consumer.logging.DatapointLogFormatter;
import com.emikaelsilveira.anomalydetector.consumer.metrics.ConsumerMetrics;
import com.emikaelsilveira.anomalydetector.consumer.validation.DatapointValidator;
import com.emikaelsilveira.anomalydetector.consumer.validation.InvalidDatapointException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatapointProcessorTest {

    private static final Instant NOW = Instant.parse("2026-08-05T14:22:07.361Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    private SimpleMeterRegistry registry;
    private BoundedIdCache idCache;
    private ListAppender<ILoggingEvent> processorAppender;
    private ListAppender<ILoggingEvent> eventAppender;
    private Logger processorLogger;
    private Logger eventLogger;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        idCache = new BoundedIdCache(500);
        processorLogger = (Logger) LoggerFactory.getLogger(DatapointProcessor.class);
        eventLogger = (Logger) LoggerFactory.getLogger(DatapointEventLogger.class);
        processorAppender = appenderFor(processorLogger);
        eventAppender = appenderFor(eventLogger);
    }

    @AfterEach
    void tearDown() {
        detach(processorLogger, processorAppender);
        detach(eventLogger, eventAppender);
        registry.close();
    }

    @Test
    void validatesBeforeDuplicateStateAndLeavesInvalidDatapointsUnremembered() {
        DatapointProcessor processor = processor();
        UUID id = UUID.randomUUID();

        assertThatThrownBy(() -> processor.process(new Datapoint(id, 0L, "sensor.temperature", 1.0d, NOW)))
                .isInstanceOf(InvalidDatapointException.class);

        assertThat(idCache.contains(id)).isFalse();
        assertThat(registry.get("anomaly.detector.points.processed").counter().count()).isZero();
        assertThat(registry.get("anomaly.detector.points.duplicates").counter().count()).isZero();
    }

    @Test
    void shortCircuitsDuplicateBeforeSequenceAndDetectorState() {
        DatapointProcessor processor = processor();
        UUID id = UUID.randomUUID();

        assertThat(processor.process(datapoint(id, 1L, 0.0d, NOW))).isEqualTo(ProcessingOutcome.PROCESSED);
        assertThat(processor.process(datapoint(id, 99L, 999.0d, NOW))).isEqualTo(ProcessingOutcome.DUPLICATE);
        assertThat(processor.process(datapoint(UUID.randomUUID(), 2L, 1.0d, NOW))).isEqualTo(ProcessingOutcome.PROCESSED);

        assertThat(registry.get("anomaly.detector.points.processed").counter().count()).isEqualTo(2.0d);
        assertThat(registry.get("anomaly.detector.points.duplicates").counter().count()).isEqualTo(1.0d);
        assertThat(messages(processorAppender)).noneMatch(message -> message.contains("Sequence"));
        assertThat(messages(eventAppender)).hasSize(2);
        // A duplicate is dropped silently from the verdict stream, so the counter is the only other
        // trace of it; the drop has to be legible in the log too, not merely tallied.
        assertThat(messages(processorAppender)).anyMatch(message ->
                message.contains("Duplicate delivery ignored")
                        && message.contains("id=" + id)
                        && message.contains("sequence=99"));
    }

    @Test
    void processesGapsAndOutOfOrderDatapointsInArrivalOrder() {
        DatapointProcessor processor = processor();

        processor.process(datapoint(UUID.randomUUID(), 1L, 0.0d, NOW));
        processor.process(datapoint(UUID.randomUUID(), 3L, 1.0d, NOW));
        processor.process(datapoint(UUID.randomUUID(), 2L, 2.0d, NOW));

        assertThat(messages(processorAppender))
                .anyMatch(message -> message.contains("expected=2") && message.contains("actual=3"))
                .anyMatch(message -> message.contains("expected=4") && message.contains("actual=2"));
        assertThat(registry.get("anomaly.detector.points.processed").counter().count()).isEqualTo(3.0d);
        // The gauge tracks the reference window each verdict was scored against, matching the
        // "Samples"/"Window" figure in the log line, so it trails admission by the current point.
        assertThat(registry.get("anomaly.detector.window.occupancy").gauge().value()).isEqualTo(2.0d);
    }

    @Test
    void warnsWhenEventTimeDriftExceedsFiveSecondsWithoutSkippingDetection() {
        DatapointProcessor processor = processor();
        Instant stale = NOW.minusSeconds(6L);

        assertThat(processor.process(datapoint(UUID.randomUUID(), 1L, 0.0d, stale)))
                .isEqualTo(ProcessingOutcome.PROCESSED);

        assertThat(messages(processorAppender)).singleElement().satisfies(message ->
                assertThat(message).contains("Event-time drift").contains("6000ms")
        );
        assertThat(messages(eventAppender)).singleElement().satisfies(message ->
                assertThat(message).contains("WARMING_UP")
        );
    }

    @Test
    void logsAnomaliesAndUpdatesTheAnomalyMetric() {
        DatapointProcessor processor = processor();

        processor.process(datapoint(UUID.randomUUID(), 1L, 0.0d, NOW));
        processor.process(datapoint(UUID.randomUUID(), 2L, 1.0d, NOW));
        processor.process(datapoint(UUID.randomUUID(), 3L, 100.0d, NOW));

        assertThat(messages(eventAppender)).anyMatch(message -> message.contains("ANOMALY DETECTED!"));
        assertThat(registry.get("anomaly.detector.points.anomalies").counter().count()).isEqualTo(1.0d);
    }

    @Test
    void recordsWarmUpResultsAndRemembersOnlyAfterSuccessfulProcessing() {
        DatapointProcessor processor = processor();
        UUID id = UUID.randomUUID();

        assertThat(processor.process(datapoint(id, 1L, 0.0d, NOW))).isEqualTo(ProcessingOutcome.PROCESSED);

        assertThat(messages(eventAppender)).singleElement().satisfies(message -> assertThat(message).contains("WARMING_UP"));
        assertThat(idCache.contains(id)).isTrue();
        assertThat(processor.process(datapoint(id, 1L, 0.0d, NOW))).isEqualTo(ProcessingOutcome.DUPLICATE);
    }

    @Test
    void keepsRejectedDatapointsOutOfAllProcessingState() {
        DatapointProcessor processor = processor();

        assertThatThrownBy(() -> processor.process(new Datapoint(
                UUID.randomUUID(),
                1L,
                "other.metric",
                1.0d,
                NOW
        ))).isInstanceOf(InvalidDatapointException.class);
        processor.process(datapoint(UUID.randomUUID(), 1L, 0.0d, NOW));
        processor.process(datapoint(UUID.randomUUID(), 2L, 1.0d, NOW));

        assertThat(messages(processorAppender)).noneMatch(message -> message.contains("Sequence"));
        assertThat(registry.get("anomaly.detector.points.processed").counter().count()).isEqualTo(2.0d);
    }

    private DatapointProcessor processor() {
        return new DatapointProcessor(
                new DatapointValidator(),
                idCache,
                new SequenceTracker(),
                new EventTimeMonitor(CLOCK),
                new ZScoreDetector(50, 2, 3.0d, true, 5),
                new DatapointEventLogger(eventLogger, new DatapointLogFormatter()),
                new ConsumerMetrics(registry, CLOCK, 100, 50)
        );
    }

    private Datapoint datapoint(UUID id, long sequence, double value, Instant emittedAt) {
        return new Datapoint(id, sequence, "sensor.temperature", value, emittedAt);
    }

    private ListAppender<ILoggingEvent> appenderFor(Logger logger) {
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        return appender;
    }

    private void detach(Logger logger, ListAppender<ILoggingEvent> appender) {
        logger.detachAppender(appender);
        appender.stop();
    }

    private List<String> messages(ListAppender<ILoggingEvent> appender) {
        return appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }
}
