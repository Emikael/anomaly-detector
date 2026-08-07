package com.emikaelsilveira.anomalydetector.producer.generation;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.emikaelsilveira.anomalydetector.producer.generation.GenerationProfile.AnomalyProfile;
import com.emikaelsilveira.anomalydetector.producer.generation.GenerationProfile.LevelShiftProfile;
import com.emikaelsilveira.anomalydetector.producer.generation.GenerationProfile.NoiseProfile;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

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
    void injectsAConstantMagnitudeWhenAnomalySigmaBoundsAreEqual() {
        DatapointGenerator generator = new DatapointGenerator(
                new Random(17),
                Clock.fixed(Instant.parse("2026-08-05T14:22:07.361Z"), ZoneOffset.UTC),
                () -> new UUID(0, 1),
                new GenerationProfile(
                        new NoiseProfile(100.0, 5.0),
                        new AnomalyProfile(1.0, 10.0, 10.0),
                        LevelShiftProfile.disabled()
                )
        );

        GeneratedDatapoint generated = generator.next();

        double signedSigma = generated.anomalyInjection().orElseThrow().signedSigma();
        assertThat(Math.abs(signedSigma)).isEqualTo(10.0);
        assertThat(generated.datapoint().value()).isEqualTo(100.0 + signedSigma * 5.0);
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

    /**
     * Pins the exact stream the README's regime-shift walkthrough describes. The committed defaults are
     * deterministic on purpose, so the documented sequence numbers are a testable claim: sequence 402 is
     * a low injection that lands near the pre-shift mean, which is what interrupts the first anomaly run
     * and delays the K=5 admission. Change a producer default and this fails before the README goes stale.
     */
    @Test
    void documentedShiftDemoInterruptsTheFirstRunWithALowInjectionAtSequence402() {
        DatapointGenerator generator = committedDefaultsGenerator();

        GeneratedDatapoint shift = generatedAt(generator, 400);
        GeneratedDatapoint first = generator.next();
        GeneratedDatapoint masked = generator.next();

        assertThat(shift.levelShift()).contains(new LevelShift(400, 100.0, 150.0, 10.0));
        assertThat(first.datapoint().sequence()).isEqualTo(401);
        assertThat(first.anomalyInjection()).isEmpty();
        assertThat(first.datapoint().value()).isCloseTo(148.37, offset(0.005));

        assertThat(masked.datapoint().sequence()).isEqualTo(402);
        assertThat(masked.anomalyInjection()).isPresent();
        assertThat(masked.anomalyInjection().orElseThrow().signedSigma()).isNegative();
        // Roughly the old baseline, so the consumer's stale reference cannot distinguish it.
        assertThat(masked.datapoint().value()).isCloseTo(90.35, offset(0.005));
    }

    private DatapointGenerator committedDefaultsGenerator() {
        AtomicLong ids = new AtomicLong();
        return new DatapointGenerator(
                new Random(42),
                Clock.fixed(Instant.parse("2026-08-05T14:22:07.361Z"), ZoneOffset.UTC),
                () -> new UUID(0, ids.incrementAndGet()),
                new GenerationProfile(
                        new NoiseProfile(100.0, 5.0),
                        new AnomalyProfile(0.02, 8.0, 12.0),
                        new LevelShiftProfile(true, 400L, 10.0)
                )
        );
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
                new GenerationProfile(
                        new NoiseProfile(100.0, 5.0),
                        new AnomalyProfile(probability, 8.0, 12.0),
                        new LevelShiftProfile(levelShiftEnabled, levelShiftAtSequence, 10.0)
                )
        );
    }
}
