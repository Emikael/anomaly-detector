package com.emikaelsilveira.anomalydetector.producer.generation;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class DatapointGeneratorInjectionTest {

    private static final ObjectMapper JSON = JsonMapper.builder().findAndAddModules().build();

    @Test
    void injectsBothHighAndLowAnomalies() {
        DatapointGenerator generator = generator(new Random(42), 1.0, false, 400);
        boolean sawHigh = false;
        boolean sawLow = false;

        for (int index = 0; index < 100; index++) {
            double signedSigma = generator.next().anomalyInjection().orElseThrow().signedSigma();
            sawHigh |= signedSigma > 0.0;
            sawLow |= signedSigma < 0.0;
        }

        assertThat(sawHigh).isTrue();
        assertThat(sawLow).isTrue();
    }

    @Test
    void keepsInjectedMagnitudeWithinConfiguredBounds() {
        DatapointGenerator generator = generator(new Random(7), 1.0, false, 400);

        for (int index = 0; index < 100; index++) {
            double signedSigma = generator.next().anomalyInjection().orElseThrow().signedSigma();
            assertThat(Math.abs(signedSigma)).isBetween(8.0, 12.0);
        }
    }

    @Test
    void serializedWireDatapointHasNoGroundTruthMetadata() throws Exception {
        DatapointGenerator generator = generator(new Random(2), 1.0, false, 400);

        String json = JSON.writeValueAsString(generator.next().datapoint());

        assertThat(json).doesNotContain("syntheticAnomaly", "anomalyInjection", "levelShift");
    }

    @Test
    void recordsTheLevelShiftAfterSequenceFourHundredUsesTheOldMean() {
        Random random = new Random(42);
        DatapointGenerator generator = generator(random, 0.0, true, 400);
        double expectedBoundaryValue = baselineAt(new Random(42), 400, 100.0);

        GeneratedDatapoint beforeBoundary = generatedAt(generator, 399);
        GeneratedDatapoint boundary = generator.next();

        assertThat(beforeBoundary.levelShift()).isEmpty();
        assertThat(boundary.datapoint().sequence()).isEqualTo(400);
        assertThat(boundary.levelShift()).contains(new LevelShift(400, 100.0, 150.0, 10.0));
        assertThat(boundary.datapoint().value()).isEqualTo(expectedBoundaryValue);
    }

    @Test
    void keepsTheShiftedMeanForLaterInjectionValuesAndRecordsTheShiftOnce() {
        DatapointGenerator generator = generator(new Random(99), 1.0, true, 2);

        GeneratedDatapoint boundary = generatedAt(generator, 2);
        GeneratedDatapoint afterBoundary = generator.next();
        double signedSigma = afterBoundary.anomalyInjection().orElseThrow().signedSigma();

        assertThat(boundary.levelShift()).contains(new LevelShift(2, 100.0, 150.0, 10.0));
        assertThat(afterBoundary.levelShift()).isEmpty();
        assertThat(afterBoundary.datapoint().sequence()).isEqualTo(3);
        assertThat(afterBoundary.datapoint().value()).isEqualTo(150.0 + signedSigma * 5.0);
    }
    @Test
    void keepsTheShiftedMeanForSubsequentBaselineValues() {
        DatapointGenerator generator = generator(new Random(23), 0.0, true, 2);
        Random expectedRandom = new Random(23);

        generator.next();
        generator.next();
        GeneratedDatapoint afterBoundary = generator.next();
        nextBaseline(expectedRandom, 100.0);
        nextBaseline(expectedRandom, 100.0);
        double expectedValue = nextBaseline(expectedRandom, 150.0);

        assertThat(afterBoundary.datapoint().value()).isEqualTo(expectedValue);
        assertThat(afterBoundary.levelShift()).isEmpty();
    }


    private GeneratedDatapoint generatedAt(DatapointGenerator generator, int sequence) {
        GeneratedDatapoint generated = null;
        for (int index = 0; index < sequence; index++) {
            generated = generator.next();
        }
        return generated;
    }
    private double baselineAt(Random random, int sequence, double mean) {
        double value = 0.0;
        for (int index = 0; index < sequence; index++) {
            value = nextBaseline(random, mean);
        }
        return value;
    }

    private double nextBaseline(Random random, double mean) {
        double value = mean + random.nextGaussian() * 5.0;
        random.nextDouble();
        return value;
    }


    private DatapointGenerator generator(Random random, double probability, boolean levelShiftEnabled, long levelShiftAtSequence) {
        AtomicLong ids = new AtomicLong();
        return new DatapointGenerator(
                random,
                Clock.fixed(Instant.parse("2026-08-05T14:22:07.361Z"), ZoneOffset.UTC),
                () -> new UUID(0, ids.incrementAndGet()),
                100.0,
                5.0,
                probability,
                8.0,
                12.0,
                levelShiftEnabled,
                levelShiftAtSequence,
                10.0
        );
    }
}
