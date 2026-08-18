package com.emikaelsilveira.anomalydetector.consumer.processing;

/** Tracks the sequence high-water mark and classifies gaps or out-of-order arrivals without reordering. */
public final class SequenceTracker {

    private boolean hasObservedSequence;
    private long lastSequence;

    public SequenceObservation observe(long sequence) {
        if (!hasObservedSequence) {
            hasObservedSequence = true;
            lastSequence = sequence;
            return new SequenceObservation(SequenceObservation.Status.FIRST, sequence, sequence);
        }

        long expectedSequence = expectedNextSequence();
        if (lastSequence != Long.MAX_VALUE && sequence == expectedSequence) {
            lastSequence = sequence;
            return new SequenceObservation(SequenceObservation.Status.IN_ORDER, expectedSequence, sequence);
        }
        if (sequence > lastSequence) {
            lastSequence = sequence;
            return new SequenceObservation(SequenceObservation.Status.GAP, expectedSequence, sequence);
        }
        return new SequenceObservation(SequenceObservation.Status.OUT_OF_ORDER, expectedSequence, sequence);
    }

    private long expectedNextSequence() {
        return lastSequence == Long.MAX_VALUE ? Long.MAX_VALUE : lastSequence + 1L;
    }
}
