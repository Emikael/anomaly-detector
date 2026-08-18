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
import com.emikaelsilveira.anomalydetector.producer.generation.GenerationProfile.AnomalyProfile;
import com.emikaelsilveira.anomalydetector.producer.generation.GenerationProfile.LevelShiftProfile;
import lombok.NonNull;

/** Generates the ordered synthetic signal while tracking sequence and permanent mean-shift state. */
public final class DatapointGenerator {

    public static final String METRIC = "sensor.temperature";

    private final Random random;
    private final Clock clock;
    private final Supplier<UUID> idSupplier;
    private final GenerationProfile profile;

    private double currentMean;
    private long sequence;

    public DatapointGenerator(
            @NonNull Random random,
            @NonNull Clock clock,
            @NonNull Supplier<UUID> idSupplier,
            @NonNull GenerationProfile profile
    ) {
        this.random = random;
        this.clock = clock;
        this.idSupplier = idSupplier;
        this.profile = profile;
        this.currentMean = profile.noise().mean();
    }

    public GeneratedDatapoint next() {
        long nextSequence = ++sequence;
        double meanBeforeShift = currentMean;
        double stddev = profile.noise().stddev();
        // Drawn unconditionally, then discarded when an anomaly wins. The order of calls into
        // `random` is part of this generator's contract: producer.seed promises a reproducible run,
        // and sampling the Gaussian only on the baseline branch would shift every subsequent draw.
        double baseline = meanBeforeShift + random.nextGaussian() * stddev;
        Optional<AnomalyInjection> anomalyInjection = anomalyInjection();
        double value = anomalyInjection
                .map(injection -> meanBeforeShift + injection.signedSigma() * stddev)
                .orElse(baseline);
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
        AnomalyProfile anomaly = profile.anomaly();
        if (random.nextDouble() >= anomaly.probability()) {
            return Optional.empty();
        }
        double magnitude = anomaly.sigmaMin() == anomaly.sigmaMax()
                ? anomaly.sigmaMin()
                : random.nextDouble(anomaly.sigmaMin(), anomaly.sigmaMax());
        double signedSigma = random.nextBoolean() ? magnitude : -magnitude;
        return Optional.of(new AnomalyInjection(signedSigma));
    }

    private Optional<LevelShift> levelShift(long nextSequence, double meanBeforeShift) {
        LevelShiftProfile levelShift = profile.levelShift();
        if (!levelShift.enabled() || nextSequence != levelShift.atSequence()) {
            return Optional.empty();
        }
        double newMean = meanBeforeShift + levelShift.sigma() * profile.noise().stddev();
        currentMean = newMean;
        return Optional.of(new LevelShift(nextSequence, meanBeforeShift, newMean, levelShift.sigma()));
    }
}
