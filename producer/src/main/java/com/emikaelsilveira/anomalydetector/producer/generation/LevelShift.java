package com.emikaelsilveira.anomalydetector.producer.generation;

public record LevelShift(long sequence, double oldMean, double newMean, double sigma) {
}
