package com.emikaelsilveira.anomalydetector.producer.messaging;

import java.util.Objects;

import com.emikaelsilveira.anomalydetector.producer.contract.Datapoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

public final class DatapointPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatapointPublisher.class);

    private final RabbitTemplate rabbitTemplate;

    public DatapointPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = Objects.requireNonNull(rabbitTemplate, "rabbitTemplate");
    }

    public void publish(Datapoint datapoint) {
        Objects.requireNonNull(datapoint, "datapoint");
        String id = datapoint.id().toString();
        try {
            rabbitTemplate.convertAndSend(
                    RabbitTopology.METRICS_EXCHANGE,
                    RabbitTopology.METRICS_ROUTING_KEY,
                    datapoint,
                    message -> {
                        message.getMessageProperties().setMessageId(id);
                        message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                        return message;
                    },
                    new CorrelationData(id)
            );
        } catch (AmqpException exception) {
            LOGGER.error("Unable to publish datapoint id={} sequence={}", id, datapoint.sequence(), exception);
            throw exception;
        }
    }
}
