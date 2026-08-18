package com.emikaelsilveira.anomalydetector.consumer.detection;

/** Enumerates the detector's warm-up, unscorable, normal, and anomalous verdicts. */
public enum DetectionStatus {
    WARMING_UP,
    DEGENERATE_WINDOW,
    OK,
    ANOMALY
}
