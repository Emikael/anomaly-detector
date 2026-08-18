package com.emikaelsilveira.anomalydetector.producer.messaging;

import com.emikaelsilveira.anomalydetector.producer.contract.Datapoint;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/** Publishes datapoints persistently to the metrics exchange with message and confirm correlation IDs. */
@RequiredArgsConstructor
public final class DatapointPublisher {

    private final @NonNull RabbitTemplate rabbitTemplate;

    /**
     * Publishes persistently with the datapoint id as both message id and confirm correlation.
     *
     * <p>Failures propagate untouched: the caller owns the decision about a lost tick, and logging
     * here as well would emit the same stack trace twice per failed publish.
     */
    public void publish(@NonNull Datapoint datapoint) {
        String id = datapoint.id().toString();
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
    }
}
