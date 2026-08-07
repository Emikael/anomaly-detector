package com.emikaelsilveira.anomalydetector.consumer.messaging;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AcknowledgingRepublishMessageRecovererTest {

    @Test
    void confirmedRepublishCreatesOnePersistentDlqCopyThenSignalsImmediateAck() {
        RabbitTemplate template = mock(RabbitTemplate.class);
        doAnswer(invocation -> {
            CorrelationData correlation = invocation.getArgument(3);
            correlation.getFuture().complete(new CorrelationData.Confirm(true, null));
            return null;
        }).when(template).send(
                eq(RabbitTopology.METRICS_DLX),
                eq(RabbitTopology.DATAPOINT_DLQ),
                any(Message.class),
                any(CorrelationData.class)
        );
        AcknowledgingRepublishMessageRecoverer recoverer = new AcknowledgingRepublishMessageRecoverer(template);
        Message original = new Message(new byte[] {1, 2, 3}, new MessageProperties());

        assertThatThrownBy(() -> recoverer.recover(original, new IllegalStateException("processor failed")))
                .isInstanceOf(ImmediateAcknowledgeAmqpException.class);

        verify(template).send(
                eq(RabbitTopology.METRICS_DLX),
                eq(RabbitTopology.DATAPOINT_DLQ),
                org.mockito.ArgumentMatchers.argThat(message ->
                        message.getMessageProperties().getDeliveryMode() == MessageDeliveryMode.PERSISTENT),
                any(CorrelationData.class)
        );
    }

    @Test
    void failedConfirmedRepublishRejectsTheOriginalManualDeliveryWithoutRequeue() {
        RabbitTemplate template = mock(RabbitTemplate.class);
        doThrow(new AmqpException("dlq unavailable")).when(template).send(
                eq(RabbitTopology.METRICS_DLX),
                eq(RabbitTopology.DATAPOINT_DLQ),
                any(Message.class),
                any(CorrelationData.class)
        );
        AcknowledgingRepublishMessageRecoverer recoverer = new AcknowledgingRepublishMessageRecoverer(template);

        assertThatThrownBy(() -> recoverer.recover(new Message(new byte[0]), new IllegalStateException("processor failed")))
                .isInstanceOfSatisfying(AmqpRejectAndDontRequeueException.class,
                        exception -> assertThat(exception.isRejectManual()).isTrue())
                .hasCauseInstanceOf(AmqpException.class);
    }
}
