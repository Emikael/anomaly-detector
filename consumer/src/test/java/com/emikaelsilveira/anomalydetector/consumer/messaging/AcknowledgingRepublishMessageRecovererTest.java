package com.emikaelsilveira.anomalydetector.consumer.messaging;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;
import com.emikaelsilveira.anomalydetector.consumer.metrics.ConsumerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConversionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

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
        AcknowledgingRepublishMessageRecoverer recoverer = new AcknowledgingRepublishMessageRecoverer(template, fatalHandler());
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
        AcknowledgingRepublishMessageRecoverer recoverer = new AcknowledgingRepublishMessageRecoverer(template, fatalHandler());

        assertThatThrownBy(() -> recoverer.recover(new Message(new byte[0]), new IllegalStateException("processor failed")))
                .isInstanceOfSatisfying(AmqpRejectAndDontRequeueException.class,
                        exception -> assertThat(exception.isRejectManual()).isTrue())
                .hasCauseInstanceOf(AmqpException.class);
    }
    @Test
    void fatalConversionFailuresRejectTheOriginalWithoutRetryRepublishingAndIncrementRejected() {
        RabbitTemplate template = mock(RabbitTemplate.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        AcknowledgingRepublishMessageRecoverer recoverer = new AcknowledgingRepublishMessageRecoverer(
                template,
                new FatalMessageErrorHandler(new ConsumerMetrics(
                        registry,
                        Clock.fixed(Instant.parse("2026-08-05T14:22:07.361Z"), ZoneOffset.UTC),
                        100,
                        50
                ))
        );

        assertThatThrownBy(() -> recoverer.recover(new Message(new byte[0]), new MessageConversionException("bad json")))
                .isInstanceOfSatisfying(AmqpRejectAndDontRequeueException.class,
                        exception -> assertThat(exception.isRejectManual()).isTrue());

        verifyNoInteractions(template);
        assertThat(registry.get("anomaly.detector.points.rejected").counter().count()).isEqualTo(1.0d);
        registry.close();
    }

    private FatalMessageErrorHandler fatalHandler() {
        return new FatalMessageErrorHandler(new ConsumerMetrics(
                new SimpleMeterRegistry(),
                Clock.systemUTC(),
                100,
                50
        ));
    }
}
