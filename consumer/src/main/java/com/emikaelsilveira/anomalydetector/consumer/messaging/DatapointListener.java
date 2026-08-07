package com.emikaelsilveira.anomalydetector.consumer.messaging;

import java.io.IOException;
import java.util.Objects;

import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;
import com.emikaelsilveira.anomalydetector.consumer.metrics.ConsumerMetrics;
import com.emikaelsilveira.anomalydetector.consumer.processing.DatapointProcessor;
import com.emikaelsilveira.anomalydetector.consumer.validation.InvalidDatapointException;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;

public final class DatapointListener {

    private final DatapointProcessor processor;
    private final ConsumerMetrics metrics;

    public DatapointListener(DatapointProcessor processor, ConsumerMetrics metrics) {
        this.processor = Objects.requireNonNull(processor, "processor");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
    }

    @RabbitListener(queues = RabbitTopology.DATAPOINT_QUEUE, containerFactory = "rabbitListenerContainerFactory")
    public void receive(Datapoint datapoint, Message rawMessage, Channel channel) throws IOException {
        long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
        try {
            processor.process(datapoint);
            channel.basicAck(deliveryTag, false);
        } catch (InvalidDatapointException invalidDatapoint) {
            metrics.recordRejected();
            channel.basicReject(deliveryTag, false);
        }
    }
}
