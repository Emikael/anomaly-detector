package com.emikaelsilveira.anomalydetector.producer.messaging;

import java.time.Instant;
import java.util.UUID;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import com.emikaelsilveira.anomalydetector.producer.contract.Datapoint;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DatapointPublisherTest {

    @Test
    void publishesToTheMetricsRouteWithPersistentMessageIdAndCorrelation() {
        RabbitTemplate template = mock(RabbitTemplate.class);
        DatapointPublisher publisher = new DatapointPublisher(template);
        Datapoint datapoint = datapoint();
        ArgumentCaptor<MessagePostProcessor> postProcessor = ArgumentCaptor.forClass(MessagePostProcessor.class);
        ArgumentCaptor<CorrelationData> correlationData = ArgumentCaptor.forClass(CorrelationData.class);

        publisher.publish(datapoint);

        verify(template).convertAndSend(
                eq(RabbitTopology.METRICS_EXCHANGE),
                eq(RabbitTopology.METRICS_ROUTING_KEY),
                same(datapoint),
                postProcessor.capture(),
                correlationData.capture()
        );
        Message message = postProcessor.getValue().postProcessMessage(new Message(new byte[0], new MessageProperties()));
        assertThat(message.getMessageProperties().getDeliveryMode()).isEqualTo(MessageDeliveryMode.PERSISTENT);
        assertThat(message.getMessageProperties().getMessageId()).isEqualTo(datapoint.id().toString());
        assertThat(correlationData.getValue().getId()).isEqualTo(datapoint.id().toString());
    }

    @Test
    void leavesAFinalConnectionFailureVisibleToTheCaller() {
        RabbitTemplate template = mock(RabbitTemplate.class);
        AmqpConnectException failure = new AmqpConnectException("broker unavailable", new IllegalStateException());
        doThrow(failure).when(template).convertAndSend(
                anyString(), anyString(), any(), any(MessagePostProcessor.class), any(CorrelationData.class));
        DatapointPublisher publisher = new DatapointPublisher(template);

        assertThatThrownBy(() -> publisher.publish(datapoint())).isSameAs(failure);
    }
    @Test
    void logsFinalPublisherFailuresAtErrorLevel() {
        RabbitTemplate template = mock(RabbitTemplate.class);
        AmqpConnectException failure = new AmqpConnectException("broker unavailable", new IllegalStateException());
        doThrow(failure).when(template).convertAndSend(
                anyString(), anyString(), any(), any(MessagePostProcessor.class), any(CorrelationData.class));
        Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger(DatapointPublisher.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            assertThatThrownBy(() -> new DatapointPublisher(template).publish(datapoint())).isSameAs(failure);

            assertThat(appender.list).anySatisfy(event -> {
                assertThat(event.getLevel()).isEqualTo(Level.ERROR);
                assertThat(event.getFormattedMessage()).contains("Unable to publish datapoint");
            });
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }


    private Datapoint datapoint() {
        return new Datapoint(
                UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93"),
                1041,
                "sensor.temperature",
                100.4213,
                Instant.parse("2026-08-05T14:22:07.361Z")
        );
    }
}
