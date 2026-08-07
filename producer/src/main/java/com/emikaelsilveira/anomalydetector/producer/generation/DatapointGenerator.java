package com.emikaelsilveira.anomalydetector.producer.generation;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;

import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

import com.emikaelsilveira.anomalydetector.producer.contract.Datapoint;

public final class DatapointGenerator {

    public static final String METRIC = "sensor.temperature";

    private final Random random;
    private final Clock clock;
    private final Supplier<UUID> idSupplier;
    private final double stddev;
    private final double anomalyProbability;
    private final double anomalySigmaMin;
    private final double anomalySigmaMax;
    private final boolean levelShiftEnabled;
    private final long levelShiftAtSequence;
    private final double levelShiftSigma;
    private double currentMean;
    private long sequence;

    public DatapointGenerator(
            Random random,
            Clock clock,
            Supplier<UUID> idSupplier,
            double mean,
            double stddev,
            double anomalyProbability,
            double anomalySigmaMin,
            double anomalySigmaMax,
            boolean levelShiftEnabled,
            long levelShiftAtSequence,
            double levelShiftSigma
    ) {
        this.random = Objects.requireNonNull(random, "random");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.idSupplier = Objects.requireNonNull(idSupplier, "idSupplier");
        this.currentMean = mean;
        this.stddev = stddev;
        this.anomalyProbability = anomalyProbability;
        this.anomalySigmaMin = anomalySigmaMin;
        this.anomalySigmaMax = anomalySigmaMax;
        this.levelShiftEnabled = levelShiftEnabled;
        this.levelShiftAtSequence = levelShiftAtSequence;
        this.levelShiftSigma = levelShiftSigma;
    }

    public GeneratedDatapoint next() {
        long nextSequence = ++sequence;
        double meanBeforeShift = currentMean;
        double value = meanBeforeShift + random.nextGaussian() * stddev;
        Optional<AnomalyInjection> anomalyInjection = anomalyInjection();
        if (anomalyInjection.isPresent()) {
            value = meanBeforeShift + anomalyInjection.orElseThrow().signedSigma() * stddev;
        }
        Optional<LevelShift> levelShift = levelShift(nextSequence, meanBeforeShift);
        Instant emittedAt = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        Datapoint datapoint = new Datapoint(
                Objects.requireNonNull(idSupplier.get(), "idSupplier returned null"),
                nextSequence,
                METRIC,
                value,
                emittedAt
        );
        return new GeneratedDatapoint(datapoint, anomalyInjection, levelShift);
    }

    private Optional<AnomalyInjection> anomalyInjection() {
        if (random.nextDouble() >= anomalyProbability) {
            return Optional.empty();
        }
        double magnitude = anomalySigmaMin == anomalySigmaMax
                ? anomalySigmaMin
                : random.nextDouble(anomalySigmaMin, anomalySigmaMax);
        double signedSigma = random.nextBoolean() ? magnitude : -magnitude;
        return Optional.of(new AnomalyInjection(signedSigma));
    }

    private Optional<LevelShift> levelShift(long nextSequence, double meanBeforeShift) {
        if (!levelShiftEnabled || nextSequence != levelShiftAtSequence) {
            return Optional.empty();
        }
        double newMean = meanBeforeShift + levelShiftSigma * stddev;
        currentMean = newMean;
        return Optional.of(new LevelShift(nextSequence, meanBeforeShift, newMean, levelShiftSigma));
    }
}
