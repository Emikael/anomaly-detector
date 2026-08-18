package com.emikaelsilveira.anomalydetector.producer.generation;

/** Describes a synthetic anomaly's signed distance from the active mean in sigma units. */
public record AnomalyInjection(double signedSigma) {
}
