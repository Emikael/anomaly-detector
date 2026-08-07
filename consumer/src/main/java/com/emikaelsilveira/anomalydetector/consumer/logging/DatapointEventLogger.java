package com.emikaelsilveira.anomalydetector.consumer.logging;

import java.util.Objects;

import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;
import com.emikaelsilveira.anomalydetector.consumer.detection.DetectionResult;
import com.emikaelsilveira.anomalydetector.consumer.detection.DetectionStatus;
import org.slf4j.Logger;

public final class DatapointEventLogger {

    private final Logger logger;
    private final DatapointLogFormatter formatter;

    public DatapointEventLogger(Logger logger, DatapointLogFormatter formatter) {
        this.logger = Objects.requireNonNull(logger, "logger");
        this.formatter = Objects.requireNonNull(formatter, "formatter");
    }

    public void log(Datapoint datapoint, DetectionResult result) {
        var event = logger.atInfo()
                .addKeyValue("event", "detection")
                .addKeyValue("emittedAt", formatter.formatTimestamp(datapoint.emittedAt()))
                .addKeyValue("sequence", datapoint.sequence())
                .addKeyValue("metric", datapoint.metric())
                .addKeyValue("value", formatter.normalizedZero(datapoint.value()))
                .addKeyValue("status", result.status().name())
                .addKeyValue("zScore", formatter.structuredZScore(result))
                .addKeyValue("samples", result.samples())
                .addKeyValue("capacity", result.capacity());
        if (result.status() == DetectionStatus.ANOMALY) {
            event.addKeyValue("alert", DatapointLogFormatter.ALERT);
        }
        event.log(formatter.format(datapoint, result));
    }
}
