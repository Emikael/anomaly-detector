package com.emikaelsilveira.anomalydetector.consumer.validation;

import java.time.Instant;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatapointValidatorTest {

    private final DatapointValidator validator = new DatapointValidator();

    @Test
    void acceptsValidFiniteDatapointsIncludingNegativeAndZeroValues() {
        assertThatCode(() -> validator.validate(datapoint(-1.0d))).doesNotThrowAnyException();
        assertThatCode(() -> validator.validate(datapoint(-0.0d))).doesNotThrowAnyException();
        assertThatCode(() -> validator.validate(datapoint(0.0d))).doesNotThrowAnyException();
    }

    @Test
    void acceptsExactNumericBounds() {
        assertThatCode(() -> validator.validate(datapoint(-1e150d))).doesNotThrowAnyException();
        assertThatCode(() -> validator.validate(datapoint(1e150d))).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @MethodSource("nonFiniteValues")
    void ec08_nanAndInfinitiesAreRejected(double value) {
        assertInvalid("value", datapoint(value));
    }

    @ParameterizedTest
    @MethodSource("outOfRangeValues")
    void ec09_valueBeyondPlusOrMinus1e150IsRejected(double value) {
        assertInvalid("value", datapoint(value));
    }

    @Test
    void rejectsMissingIdMetricAndEventTimeBeforeAnyProcessingStateCanChange() {
        assertInvalid("id", mutate(point -> new Datapoint(null, point.sequence(), point.metric(), point.value(), point.emittedAt())));
        assertInvalid("metric", mutate(point -> new Datapoint(point.id(), point.sequence(), null, point.value(), point.emittedAt())));
        assertInvalid("emittedAt", mutate(point -> new Datapoint(point.id(), point.sequence(), point.metric(), point.value(), null)));
    }

    @Test
    void rejectsSequencesBeforeOne() {
        assertInvalid("sequence", new Datapoint(UUID.randomUUID(), 0L, "sensor.temperature", 1.0d, Instant.EPOCH));
        assertInvalid("sequence", new Datapoint(UUID.randomUUID(), -1L, "sensor.temperature", 1.0d, Instant.EPOCH));
    }

    @Test
    void rejectsMetricsOtherThanTheWireContractMetric() {
        assertInvalid("metric", new Datapoint(UUID.randomUUID(), 1L, "sensor.humidity", 1.0d, Instant.EPOCH));
    }

    @Test
    void rejectsANullDatapointWithAFieldSpecificMessage() {
        assertInvalid("datapoint", null);
    }

    private void assertInvalid(String field, Datapoint datapoint) {
        assertThatThrownBy(() -> validator.validate(datapoint))
                .isInstanceOf(InvalidDatapointException.class)
                .satisfies(exception -> assertThat(exception.getMessage())
                        .contains(field)
                        .doesNotContain("sensor.temperature"));
    }

    private Datapoint mutate(UnaryOperator<Datapoint> mutation) {
        return mutation.apply(datapoint(1.0d));
    }

    private Datapoint datapoint(double value) {
        return new Datapoint(
                UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93"),
                1041L,
                "sensor.temperature",
                value,
                Instant.parse("2026-08-05T14:22:03.114Z")
        );
    }

    private static Stream<Arguments> nonFiniteValues() {
        return Stream.of(
                Arguments.of(Double.NaN),
                Arguments.of(Double.NEGATIVE_INFINITY),
                Arguments.of(Double.POSITIVE_INFINITY)
        );
    }

    private static Stream<Arguments> outOfRangeValues() {
        return Stream.of(
                Arguments.of(Math.nextDown(-1e150d)),
                Arguments.of(Math.nextUp(1e150d))
        );
    }
}
