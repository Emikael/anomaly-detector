package com.emikaelsilveira.anomalydetector.consumer.messaging;

import java.io.IOException;

import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;
import com.emikaelsilveira.anomalydetector.consumer.metrics.ConsumerMetrics;
import com.emikaelsilveira.anomalydetector.consumer.processing.DatapointProcessor;
import com.emikaelsilveira.anomalydetector.consumer.validation.InvalidDatapointException;
import com.rabbitmq.client.Channel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

/** Receives datapoints, delegates ordered processing, and owns manual ack or terminal rejection. */
@Slf4j
@RequiredArgsConstructor
public final class DatapointListener {

    private final @NonNull DatapointProcessor processor;
    private final @NonNull ConsumerMetrics metrics;

    @RabbitListener(queues = RabbitTopology.DATAPOINT_QUEUE, containerFactory = "rabbitListenerContainerFactory")
    public void receive(Datapoint datapoint, Message rawMessage, Channel channel) throws IOException {
        long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
        try {
            // PROCESSED and DUPLICATE both ack: the delivery was handled either way, and requeuing a
            // duplicate would only replay it. The outcome is the processor's contract for callers and
            // tests, not a delivery-routing decision, so it is deliberately not branched on here.
            processor.process(datapoint);
            channel.basicAck(deliveryTag, false);
        } catch (InvalidDatapointException invalidDatapoint) {
            metrics.recordRejected();
            // Rejecting straight to the DLQ is deliberate — a payload this broken will never become
            // valid on redelivery — but it must be visible, not merely counted.
            LOGGER.atWarn().log(
                    "Dead-lettering invalid datapoint: field={} messageId={} sequence={}",
                    invalidDatapoint.field(),
                    rawMessage.getMessageProperties().getMessageId(),
                    datapoint == null ? null : datapoint.sequence()
            );
            channel.basicReject(deliveryTag, false);
        }
    }
}
