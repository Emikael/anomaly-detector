package com.emikaelsilveira.anomalydetector.producer.production;

import com.emikaelsilveira.anomalydetector.producer.generation.DatapointGenerator;
import com.emikaelsilveira.anomalydetector.producer.generation.GeneratedDatapoint;
import com.emikaelsilveira.anomalydetector.producer.logging.ProducerEventLogger;
import com.emikaelsilveira.anomalydetector.producer.messaging.DatapointPublisher;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.springframework.amqp.AmqpException;
import org.springframework.scheduling.annotation.Scheduled;

@RequiredArgsConstructor
public final class DatapointProductionService {

    private final @NonNull DatapointGenerator generator;
    private final @NonNull DatapointPublisher publisher;
    private final @NonNull ProducerEventLogger eventLogger;
    /** Injected, not static: the publish-failure branch is asserted through a test appender. */
    private final @NonNull Logger logger;

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
