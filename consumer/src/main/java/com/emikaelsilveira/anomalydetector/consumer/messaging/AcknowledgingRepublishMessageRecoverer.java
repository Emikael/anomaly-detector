package com.emikaelsilveira.anomalydetector.consumer.messaging;

import lombok.NonNull;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecovererWithConfirms;

/** Republishes exhausted deliveries to the DLQ with confirms before completing the original message. */
public final class AcknowledgingRepublishMessageRecoverer implements MessageRecoverer {

    private final RepublishMessageRecovererWithConfirms confirmedRecoverer;
    private final FatalMessageErrorHandler fatalMessageErrorHandler;

    public AcknowledgingRepublishMessageRecoverer(
            @NonNull RabbitTemplate rabbitTemplate,
            @NonNull FatalMessageErrorHandler fatalMessageErrorHandler
    ) {
        this.fatalMessageErrorHandler = fatalMessageErrorHandler;
        confirmedRecoverer = new RepublishMessageRecovererWithConfirms(
                rabbitTemplate,
                RabbitTopology.METRICS_DLX,
                RabbitTopology.DATAPOINT_DLQ,
                CachingConnectionFactory.ConfirmType.CORRELATED
        );
        confirmedRecoverer.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
    }

    @Override
    public void recover(Message message, Throwable cause) {
        if (fatalMessageErrorHandler.recordConversionFailure(cause)) {
            throw new AmqpRejectAndDontRequeueException("Fatal message conversion failed", true, cause);
        }
        try {
            confirmedRecoverer.recover(message, cause);
        } catch (RuntimeException republishFailure) {
            throw new AmqpRejectAndDontRequeueException(
                    "Confirmed dead-letter republish failed",
                    true,
                    republishFailure
            );
        }
        throw new ImmediateAcknowledgeAmqpException("Confirmed dead-letter republish completed");
    }
}
