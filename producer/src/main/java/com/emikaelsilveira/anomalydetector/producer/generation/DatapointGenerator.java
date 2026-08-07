package com.emikaelsilveira.anomalydetector.producer.generation;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

import com.emikaelsilveira.anomalydetector.producer.contract.Datapoint;

public final class DatapointGenerator {

    public static final String METRIC = "sensor.temperature";

    private final Random random;
    private final Clock clock;
    private final Supplier<UUID> idSupplier;
    private final double mean;
    private final double stddev;
    private final double anomalyProbability;
    private long sequence;

    public DatapointGenerator(
            Random random,
            Clock clock,
            Supplier<UUID> idSupplier,
            double mean,
            double stddev,
            double anomalyProbability
    ) {
        this.random = Objects.requireNonNull(random, "random");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.idSupplier = Objects.requireNonNull(idSupplier, "idSupplier");
        this.mean = mean;
        this.stddev = stddev;
        this.anomalyProbability = anomalyProbability;
    }

    public GeneratedDatapoint next() {
        long nextSequence = ++sequence;
        double value = mean + random.nextGaussian() * stddev;
        Instant emittedAt = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        Datapoint datapoint = new Datapoint(idSupplier.get(), nextSequence, METRIC, value, emittedAt);
        return new GeneratedDatapoint(datapoint);
    }
}
