package com.emikaelsilveira.anomalydetector.consumer.processing;

/** Distinguishes newly processed datapoints from acknowledged duplicate deliveries. */
public enum ProcessingOutcome {
    PROCESSED,
    DUPLICATE
}
