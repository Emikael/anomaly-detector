package com.emikaelsilveira.anomalydetector.consumer.processing;

import java.time.Duration;
import java.util.Objects;

import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;
import com.emikaelsilveira.anomalydetector.consumer.detection.DetectionResult;
import com.emikaelsilveira.anomalydetector.consumer.detection.ZScoreDetector;
import com.emikaelsilveira.anomalydetector.consumer.logging.DatapointEventLogger;
import com.emikaelsilveira.anomalydetector.consumer.metrics.ConsumerMetrics;
import com.emikaelsilveira.anomalydetector.consumer.validation.DatapointValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DatapointProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatapointProcessor.class);

    private final DatapointValidator validator;
    private final BoundedIdCache idCache;
    private final SequenceTracker sequenceTracker;
    private final EventTimeMonitor eventTimeMonitor;
    private final ZScoreDetector detector;
    private final DatapointEventLogger eventLogger;
    private final ConsumerMetrics metrics;

    public DatapointProcessor(
            DatapointValidator validator,
            BoundedIdCache idCache,
            SequenceTracker sequenceTracker,
            EventTimeMonitor eventTimeMonitor,
            ZScoreDetector detector,
            DatapointEventLogger eventLogger,
            ConsumerMetrics metrics
    ) {
        this.validator = Objects.requireNonNull(validator, "validator");
        this.idCache = Objects.requireNonNull(idCache, "idCache");
        this.sequenceTracker = Objects.requireNonNull(sequenceTracker, "sequenceTracker");
        this.eventTimeMonitor = Objects.requireNonNull(eventTimeMonitor, "eventTimeMonitor");
        this.detector = Objects.requireNonNull(detector, "detector");
        this.eventLogger = Objects.requireNonNull(eventLogger, "eventLogger");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
    }

    public ProcessingOutcome process(Datapoint datapoint) {
        long startedAt = System.nanoTime();
        validator.validate(datapoint);
        if (idCache.contains(datapoint.id())) {
            metrics.recordDuplicate();
            return ProcessingOutcome.DUPLICATE;
        }

        warnOnSequence(sequenceTracker.observe(datapoint.sequence()));
        warnOnEventTimeDrift(datapoint);
        DetectionResult result = detector.evaluate(datapoint.value());
        eventLogger.log(datapoint, result);
        metrics.recordProcessed(result, Duration.ofNanos(System.nanoTime() - startedAt));
        idCache.remember(datapoint.id());
        return ProcessingOutcome.PROCESSED;
    }

    private void warnOnSequence(SequenceObservation observation) {
        switch (observation.status()) {
            case GAP -> LOGGER.atWarn().log(
                    "Sequence gap: expected={} actual={}",
                    observation.expectedSequence(),
                    observation.actualSequence()
            );
            case OUT_OF_ORDER -> LOGGER.atWarn().log(
                    "Sequence out of order: expected={} actual={}",
                    observation.expectedSequence(),
                    observation.actualSequence()
            );
            case FIRST, IN_ORDER -> {
            }
        }
    }

    private void warnOnEventTimeDrift(Datapoint datapoint) {
        if (eventTimeMonitor.isDrifted(datapoint.emittedAt())) {
            LOGGER.atWarn().log("Event-time drift: {}ms", eventTimeMonitor.drift(datapoint.emittedAt()).toMillis());
        }
    }
}
