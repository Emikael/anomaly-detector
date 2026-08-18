package com.emikaelsilveira.anomalydetector.consumer.validation;

import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;

/** Enforces the consumer's semantic datapoint contract after JSON conversion. */
public final class DatapointValidator {

    private static final String METRIC = "sensor.temperature";
    private static final double MAX_ABSOLUTE_VALUE = 1e150d;

    /**
     * Throws on the first violation, naming the offending field. Each check throws inline rather
     * than delegating to a {@code reject(field)} helper: a call that only ever throws still reads as
     * one that might return, which makes every dereference below it look unguarded.
     */
    public void validate(Datapoint datapoint) {
        if (datapoint == null) {
            throw new InvalidDatapointException("datapoint");
        }
        if (datapoint.id() == null) {
            throw new InvalidDatapointException("id");
        }
        if (datapoint.sequence() < 1L) {
            throw new InvalidDatapointException("sequence");
        }
        if (!METRIC.equals(datapoint.metric())) {
            throw new InvalidDatapointException("metric");
        }
        if (isOutsideSafeRange(datapoint.value())) {
            throw new InvalidDatapointException("value");
        }
        if (datapoint.emittedAt() == null) {
            throw new InvalidDatapointException("emittedAt");
        }
    }

    /** Mirrors the producer's own envelope: anything wider cannot have come from a valid generator. */
    private static boolean isOutsideSafeRange(double value) {
        return !Double.isFinite(value) || Math.abs(value) > MAX_ABSOLUTE_VALUE;
    }
}
