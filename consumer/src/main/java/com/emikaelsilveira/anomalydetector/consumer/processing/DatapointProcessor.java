package com.emikaelsilveira.anomalydetector.consumer.processing;

import java.time.Duration;

import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;
import com.emikaelsilveira.anomalydetector.consumer.detection.DetectionResult;
import com.emikaelsilveira.anomalydetector.consumer.detection.ZScoreDetector;
import com.emikaelsilveira.anomalydetector.consumer.logging.DatapointEventLogger;
import com.emikaelsilveira.anomalydetector.consumer.metrics.ConsumerMetrics;
import com.emikaelsilveira.anomalydetector.consumer.validation.DatapointValidator;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public final class DatapointProcessor {

    private final @NonNull DatapointValidator validator;
    private final @NonNull BoundedIdCache idCache;
    private final @NonNull SequenceTracker sequenceTracker;
    private final @NonNull EventTimeMonitor eventTimeMonitor;
    private final @NonNull ZScoreDetector detector;
    private final @NonNull DatapointEventLogger eventLogger;
    private final @NonNull ConsumerMetrics metrics;

    public ProcessingOutcome process(Datapoint datapoint) {
        long startedAt = System.nanoTime();
        validator.validate(datapoint);
        if (isDuplicate(datapoint)) {
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

    /** Counts and reports the duplicate as a side effect, so the caller reads as a plain guard. */
    private boolean isDuplicate(Datapoint datapoint) {
        if (!idCache.contains(datapoint.id())) {
            return false;
        }
        metrics.recordDuplicate();
        LOGGER.atInfo().log(
                "Duplicate delivery ignored: id={} sequence={}",
                datapoint.id(),
                datapoint.sequence()
        );
        return true;
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
        Duration drift = eventTimeMonitor.drift(datapoint.emittedAt());
        if (eventTimeMonitor.isExcessive(drift)) {
            LOGGER.atWarn().log("Event-time drift: {}ms", drift.toMillis());
        }
    }
}
