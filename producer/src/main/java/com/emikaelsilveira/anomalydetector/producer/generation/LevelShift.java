package com.emikaelsilveira.anomalydetector.producer.generation;

/** Describes the sequence boundary and mean change of a permanent synthetic level shift. */
public record LevelShift(long sequence, double oldMean, double newMean, double sigma) {
}
