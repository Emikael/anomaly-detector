package com.emikaelsilveira.anomalydetector.producer.config;

import java.time.Clock;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

import com.emikaelsilveira.anomalydetector.producer.generation.DatapointGenerator;
import com.emikaelsilveira.anomalydetector.producer.logging.ProducerEventLogger;
import com.emikaelsilveira.anomalydetector.producer.messaging.DatapointPublisher;
import com.emikaelsilveira.anomalydetector.producer.production.DatapointProductionService;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Composes the producer's generator, publisher, logging, time, identity, and scheduling beans. */
@Configuration(proxyBeanMethods = false)
public class ProducerRuntimeConfiguration {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    Supplier<UUID> idSupplier() {
        return UUID::randomUUID;
    }

    @Bean
    Random random(ProducerProperties properties) {
        return properties.seed() == null ? new Random() : new Random(properties.seed());
    }

    @Bean
    DatapointGenerator datapointGenerator(
            Random random,
            Clock clock,
            Supplier<UUID> idSupplier,
            ProducerProperties properties
    ) {
        return new DatapointGenerator(random, clock, idSupplier, properties.generationProfile());
    }

    @Bean
    ProducerEventLogger producerEventLogger() {
        return new ProducerEventLogger(LoggerFactory.getLogger(ProducerEventLogger.class));
    }

    @Bean
    DatapointPublisher datapointPublisher(RabbitTemplate rabbitTemplate) {
        return new DatapointPublisher(rabbitTemplate);
    }

    /**
     * Own namespace, not {@code spring.task.scheduling.*}. Spring Boot defines no {@code enabled} key
     * there, so squatting on the framework's prefix would hide a private switch behind a name that
     * looks like a documented one.
     */
    @Bean
    @ConditionalOnProperty(
            name = "producer.scheduling-enabled",
            havingValue = "true",
            matchIfMissing = true
    )
    DatapointProductionService datapointProductionService(
            DatapointGenerator generator,
            DatapointPublisher publisher,
            ProducerEventLogger eventLogger
    ) {
        return new DatapointProductionService(
                generator,
                publisher,
                eventLogger,
                LoggerFactory.getLogger(DatapointProductionService.class)
        );
    }
}
