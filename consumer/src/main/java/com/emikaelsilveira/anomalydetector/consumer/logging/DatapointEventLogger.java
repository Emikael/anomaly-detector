package com.emikaelsilveira.anomalydetector.consumer.logging;

import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;
import com.emikaelsilveira.anomalydetector.consumer.detection.DetectionResult;
import com.emikaelsilveira.anomalydetector.consumer.detection.DetectionStatus;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;

/**
 * Takes its {@link Logger} by injection rather than declaring a static one, so tests can assert on
 * the character-exact verdict line R11 mandates without touching global logging state.
 */
@RequiredArgsConstructor
public final class DatapointEventLogger {

    private final @NonNull Logger logger;
    private final @NonNull DatapointLogFormatter formatter;

    public void log(Datapoint datapoint, DetectionResult result) {
        var event = logger.atInfo()
                .addKeyValue("event", "detection")
                .addKeyValue("emittedAt", formatter.formatTimestamp(datapoint.emittedAt()))
                .addKeyValue("sequence", datapoint.sequence())
                .addKeyValue("metric", datapoint.metric())
                .addKeyValue("value", formatter.normalizedZero(datapoint.value()))
                .addKeyValue("status", result.status().name())
                .addKeyValue("zScore", formatter.structuredZScore(result))
                .addKeyValue("referenceSamples", result.referenceSamples())
                .addKeyValue("capacity", result.capacity());
        if (result.status() == DetectionStatus.ANOMALY) {
            event.addKeyValue("alert", DatapointLogFormatter.ALERT);
        }
        event.log(formatter.format(datapoint, result));
    }
}
