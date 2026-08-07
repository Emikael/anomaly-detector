package com.emikaelsilveira.anomalydetector.producer.contract;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.SpecificationVersion;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class DatapointContractTest {

    private static final ObjectMapper JSON = JsonMapper.builder().findAndAddModules().build();

    @Test
    void validRoundTripMatchesSchema() throws Exception {
        Datapoint expected = datapoint();
        String json = JSON.writeValueAsString(expected);

        assertThat(schema().validate(JSON.readTree(json))).isEmpty();
        assertThat(JSON.readValue(json, Datapoint.class)).isEqualTo(expected);
    }

    @Test
    void missingRequiredFieldFailsSchemaValidation() throws Exception {
        String json = """
                {"id":"0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93","sequence":1041,"metric":"sensor.temperature","value":100.4213}
                """;

        assertThat(schema().validate(JSON.readTree(json))).isNotEmpty();
    }

    @Test
    void unknownFieldPassesSchemaAndIsIgnoredByRecord() throws Exception {
        String json = """
                {"id":"0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93","sequence":1041,"metric":"sensor.temperature","value":100.4213,"emittedAt":"2026-08-05T14:22:03.114Z","futureField":true}
                """;

        assertThat(schema().validate(JSON.readTree(json))).isEmpty();
        assertThat(JSON.readValue(json, Datapoint.class)).isEqualTo(datapoint());
    }

    @Test
    void invalidMetricValueAndFormatFailSchemaValidation() throws Exception {
        assertRejected("""
                {"id":"0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93","sequence":1041,"metric":"sensor.pressure","value":100.4213,"emittedAt":"2026-08-05T14:22:03.114Z"}
                """);
        assertRejected("""
                {"id":"0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93","sequence":1041,"metric":"sensor.temperature","value":1e151,"emittedAt":"2026-08-05T14:22:03.114Z"}
                """);
        assertRejected("""
                {"id":"not-a-uuid","sequence":1041,"metric":"sensor.temperature","value":100.4213,"emittedAt":"not-a-date-time"}
                """);
    }

    private void assertRejected(String json) throws Exception {
        assertThat(schema().validate(JSON.readTree(json))).isNotEmpty();
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

    private Datapoint datapoint() {
        return new Datapoint(
                UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93"),
                1041,
                "sensor.temperature",
                100.4213,
                Instant.parse("2026-08-05T14:22:03.114Z")
        );
    }
}
