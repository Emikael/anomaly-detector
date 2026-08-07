package com.emikaelsilveira.anomalydetector.consumer.processing;

public record SequenceObservation(Status status, long expectedSequence, long actualSequence) {

    public enum Status {
        FIRST,
        IN_ORDER,
        GAP,
        OUT_OF_ORDER
    }
}
