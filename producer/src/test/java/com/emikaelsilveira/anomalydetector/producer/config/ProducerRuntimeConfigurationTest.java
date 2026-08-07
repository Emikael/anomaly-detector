package com.emikaelsilveira.anomalydetector.producer.config;

import java.time.Clock;
import java.time.ZoneOffset;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

import com.emikaelsilveira.anomalydetector.producer.ProducerApplication;
import com.emikaelsilveira.anomalydetector.producer.generation.DatapointGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;


class ProducerRuntimeConfigurationTest {

    private final ProducerRuntimeConfiguration configuration = new ProducerRuntimeConfiguration();
    private final ApplicationContextRunner runtimeContextRunner = new ApplicationContextRunner()
            .withUserConfiguration(ProducerRuntimeConfiguration.class, RuntimeTestConfiguration.class);


    @Test
    void providesUtcClockAndUuidBoundary() {
        Clock clock = configuration.clock();
        Supplier<UUID> idSupplier = configuration.idSupplier();

        assertThat(clock.getZone()).isEqualTo(ZoneOffset.UTC);
        assertThat(idSupplier.get()).isNotNull();
    }

    @Test
    void usesTheConfiguredSeedForDeterministicRandomnessAndBlankSeedForANewRandom() {
        ProducerProperties seeded = properties(42L);
        Random expected = new Random(42L);

        assertThat(configuration.random(seeded).nextGaussian()).isEqualTo(expected.nextGaussian());
        assertThat(configuration.random(properties(null))).isInstanceOf(Random.class);
    }

    @Test
    void wiresThePureGeneratorFromProducerProperties() {
        ProducerProperties properties = properties(42L);
        Clock clock = Clock.fixed(java.time.Instant.parse("2026-08-05T14:22:07.361Z"), ZoneOffset.UTC);
        DatapointGenerator generator = configuration.datapointGenerator(
                configuration.random(properties), clock, () -> UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93"), properties);

        assertThat(generator.next().datapoint()).satisfies(datapoint -> {
            assertThat(datapoint.sequence()).isEqualTo(1);
            assertThat(datapoint.metric()).isEqualTo("sensor.temperature");
            assertThat(datapoint.emittedAt()).isEqualTo(java.time.Instant.parse("2026-08-05T14:22:07.361Z"));
        });
    }

    @Test
    void enablesSchedulingOnTheProducerApplication() {
        assertThat(ProducerApplication.class.isAnnotationPresent(EnableScheduling.class)).isTrue();
    }
    @Test
    void createsTheProductionServiceByDefaultAndOmitsItWhenSchedulingIsDisabled() {
        runtimeContextRunner.run(context ->
                assertThat(context).hasSingleBean(com.emikaelsilveira.anomalydetector.producer.production.DatapointProductionService.class));
        runtimeContextRunner.withPropertyValues("spring.task.scheduling.enabled=false").run(context ->
                assertThat(context).doesNotHaveBean(com.emikaelsilveira.anomalydetector.producer.production.DatapointProductionService.class));
    }


    private ProducerProperties properties(Long seed) {
        return new ProducerProperties(250, 100.0, 5.0, 0.02, 8.0, 12.0, seed, false, 400, 10.0);
    }
    @Configuration(proxyBeanMethods = false)
    static class RuntimeTestConfiguration {

        @Bean
        ProducerProperties producerProperties() {
            return new ProducerProperties(250, 100.0, 5.0, 0.02, 8.0, 12.0, 42L, false, 400, 10.0);
        }

        @Bean
        RabbitTemplate rabbitTemplate() {
            return mock(RabbitTemplate.class);
        }
    }

}
