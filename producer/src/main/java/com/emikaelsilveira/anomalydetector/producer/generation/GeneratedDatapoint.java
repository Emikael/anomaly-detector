package com.emikaelsilveira.anomalydetector.producer.generation;

import java.util.Objects;

import com.emikaelsilveira.anomalydetector.producer.contract.Datapoint;

public record GeneratedDatapoint(Datapoint datapoint) {

    public GeneratedDatapoint {
        Objects.requireNonNull(datapoint, "datapoint");
    }
}
