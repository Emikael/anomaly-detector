package com.emikaelsilveira.anomalydetector.consumer.processing;

/** Captures how an arriving sequence compares with the tracker's expected next value. */
public record SequenceObservation(Status status, long expectedSequence, long actualSequence) {

    /** Classifies an observation as initial, ordered, gapped, or older than the high-water mark. */
    public enum Status {
        FIRST,
        IN_ORDER,
        GAP,
        OUT_OF_ORDER
    }
}
