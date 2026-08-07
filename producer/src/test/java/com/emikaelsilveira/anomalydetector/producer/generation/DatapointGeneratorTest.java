package com.emikaelsilveira.anomalydetector.producer.generation;

import java.io.InputStream;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.emikaelsilveira.anomalydetector.producer.contract.Datapoint;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.SpecificationVersion;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class DatapointGeneratorTest {

    private static final ObjectMapper JSON = JsonMapper.builder().findAndAddModules().build();

    @Test
    void startsAtOneAndIncrementsMonotonically() {
        DatapointGenerator generator = generator(new Random(7), Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));

        assertThat(generator.next().datapoint().sequence()).isEqualTo(1);
        assertThat(generator.next().datapoint().sequence()).isEqualTo(2);
        assertThat(generator.next().datapoint().sequence()).isEqualTo(3);
    }

    @Test
    void equalSeedsProduceTheSameSequenceValueAndEventSchedule() {
        Clock clock = Clock.fixed(Instant.parse("2026-08-05T14:22:07.361987Z"), ZoneOffset.UTC);
        DatapointGenerator first = generator(new Random(42), clock);
        DatapointGenerator second = generator(new Random(42), clock);

        for (int index = 0; index < 10; index++) {
            Datapoint firstPoint = first.next().datapoint();
            Datapoint secondPoint = second.next().datapoint();

            assertThat(firstPoint.sequence()).isEqualTo(secondPoint.sequence());
            assertThat(firstPoint.value()).isEqualTo(secondPoint.value());
            assertThat(firstPoint.emittedAt()).isEqualTo(secondPoint.emittedAt());
        }
    }

    @Test
    void usesInjectedClockAndUuidAndTruncatesEventTimeToMilliseconds() {
        UUID id = UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93");
        Clock clock = Clock.fixed(Instant.parse("2026-08-05T14:22:07.361987Z"), ZoneOffset.UTC);
        DatapointGenerator generator = new DatapointGenerator(new Random(2), clock, () -> id, 100.0, 5.0, 0.0);

        Datapoint datapoint = generator.next().datapoint();

        assertThat(datapoint.id()).isEqualTo(id);
        assertThat(datapoint.emittedAt()).isEqualTo(Instant.parse("2026-08-05T14:22:07.361Z"));
        assertThat(datapoint.metric()).isEqualTo("sensor.temperature");
    }

    @Test
    void generatesTheGaussianBaselineValue() {
        long seed = 91;
        Random expectedRandom = new Random(seed);
        double expectedValue = 100.0 + expectedRandom.nextGaussian() * 5.0;
        DatapointGenerator generator = generator(new Random(seed), Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));

        assertThat(generator.next().datapoint().value()).isEqualTo(expectedValue);
    }
    @Test
    void zeroProbabilityKeepsTheGaussianBaselinePath() {
        long seed = 13;
        double expectedValue = 100.0 + new Random(seed).nextGaussian() * 5.0;
        DatapointGenerator generator = new DatapointGenerator(
                new Random(seed),
                Clock.fixed(Instant.EPOCH, ZoneOffset.UTC),
                () -> UUID.randomUUID(),
                100.0,
                5.0,
                0.0
        );

        assertThat(generator.next().datapoint().value()).isEqualTo(expectedValue);
    }


    @Test
    void generatedDatapointValidatesAgainstTheRootSchema() throws Exception {
        Datapoint datapoint = generator(new Random(42), Clock.fixed(Instant.parse("2026-08-05T14:22:07.361Z"), ZoneOffset.UTC))
                .next()
                .datapoint();

        assertThat(schema().validate(JSON.readTree(JSON.writeValueAsString(datapoint)))).isEmpty();
    }

    private DatapointGenerator generator(Random random, Clock clock) {
        AtomicLong ids = new AtomicLong();
        Supplier<UUID> idSupplier = () -> new UUID(0, ids.incrementAndGet());
        return new DatapointGenerator(random, clock, idSupplier, 100.0, 5.0, 0.0);
    }

    private Schema schema() {
        SchemaRegistryConfig config = SchemaRegistryConfig.builder().formatAssertionsEnabled(true).build();
        SchemaRegistry registry = SchemaRegistry.withDefaultDialect(
                SpecificationVersion.DRAFT_2020_12,
                builder -> builder.schemaRegistryConfig(config)
        );
        InputStream input = Objects.requireNonNull(getClass().getResourceAsStream("/datapoint.v1.schema.json"));
        return registry.getSchema(input);
    }
}
