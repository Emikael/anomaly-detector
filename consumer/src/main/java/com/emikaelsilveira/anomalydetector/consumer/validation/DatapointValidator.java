package com.emikaelsilveira.anomalydetector.consumer.validation;

import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;

public final class DatapointValidator {

    private static final String METRIC = "sensor.temperature";
    private static final double MAX_ABSOLUTE_VALUE = 1e150d;

    public void validate(Datapoint datapoint) {
        if (datapoint == null) {
            reject("datapoint");
        }
        if (datapoint.id() == null) {
            reject("id");
        }
        if (datapoint.sequence() < 1L) {
            reject("sequence");
        }
        if (!METRIC.equals(datapoint.metric())) {
            reject("metric");
        }
        if (!Double.isFinite(datapoint.value())
                || datapoint.value() < -MAX_ABSOLUTE_VALUE
                || datapoint.value() > MAX_ABSOLUTE_VALUE) {
            reject("value");
        }
        if (datapoint.emittedAt() == null) {
            reject("emittedAt");
        }
    }

    private void reject(String field) {
        throw new InvalidDatapointException(field);
    }
}
