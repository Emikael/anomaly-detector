package com.emikaelsilveira.anomalydetector.producer.production;

import java.util.Objects;

import com.emikaelsilveira.anomalydetector.producer.generation.DatapointGenerator;
import com.emikaelsilveira.anomalydetector.producer.generation.GeneratedDatapoint;
import com.emikaelsilveira.anomalydetector.producer.logging.ProducerEventLogger;
import com.emikaelsilveira.anomalydetector.producer.messaging.DatapointPublisher;
import org.slf4j.Logger;
import org.springframework.amqp.AmqpException;
import org.springframework.scheduling.annotation.Scheduled;

public final class DatapointProductionService {

    private final DatapointGenerator generator;
    private final DatapointPublisher publisher;
    private final ProducerEventLogger eventLogger;
    private final Logger logger;

    public DatapointProductionService(
            DatapointGenerator generator,
            DatapointPublisher publisher,
            ProducerEventLogger eventLogger,
            Logger logger
    ) {
        this.generator = Objects.requireNonNull(generator, "generator");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.eventLogger = Objects.requireNonNull(eventLogger, "eventLogger");
        this.logger = Objects.requireNonNull(logger, "logger");
    }

    @Scheduled(fixedRateString = "${producer.interval-ms}")
    public void produce() {
        GeneratedDatapoint generatedDatapoint = generator.next();
        try {
            publisher.publish(generatedDatapoint.datapoint());
            eventLogger.log(generatedDatapoint);
        } catch (AmqpException exception) {
            logger.error("Unable to publish generated datapoint id={} sequence={}",
                    generatedDatapoint.datapoint().id(), generatedDatapoint.datapoint().sequence(), exception);
        }
    }
}
