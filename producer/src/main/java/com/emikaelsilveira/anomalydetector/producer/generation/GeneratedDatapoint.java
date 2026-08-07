package com.emikaelsilveira.anomalydetector.producer.generation;

import java.util.Objects;
import java.util.Optional;

import com.emikaelsilveira.anomalydetector.producer.contract.Datapoint;

public record GeneratedDatapoint(
        Datapoint datapoint,
        Optional<AnomalyInjection> anomalyInjection,
        Optional<LevelShift> levelShift
) {

    public GeneratedDatapoint {
        Objects.requireNonNull(datapoint, "datapoint");
        Objects.requireNonNull(anomalyInjection, "anomalyInjection");
        Objects.requireNonNull(levelShift, "levelShift");
    }
}
