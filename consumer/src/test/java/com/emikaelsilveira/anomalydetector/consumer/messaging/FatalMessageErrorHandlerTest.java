package com.emikaelsilveira.anomalydetector.consumer.messaging;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import com.emikaelsilveira.anomalydetector.consumer.metrics.ConsumerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.amqp.support.converter.MessageConversionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FatalMessageErrorHandlerTest {

    private SimpleMeterRegistry registry;
    private FatalMessageErrorHandler errorHandler;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        errorHandler = new FatalMessageErrorHandler(new ConsumerMetrics(
                registry,
                Clock.fixed(Instant.parse("2026-08-05T14:22:07.361Z"), ZoneOffset.UTC),
                100,
                50
        ));
    }

    @AfterEach
    void tearDown() {
        registry.close();
    }

    @Test
    void countsConversionFailuresBeforeDelegatingTheirFatalRejection() {
        ListenerExecutionFailedException failure = new ListenerExecutionFailedException(
                "conversion failed",
                new MessageConversionException("missing sequence"),
                message()
        );

        assertThatThrownBy(() -> errorHandler.handleError(failure))
                .isInstanceOf(AmqpRejectAndDontRequeueException.class);

        assertThat(registry.get("anomaly.detector.points.rejected").counter().count()).isEqualTo(1.0d);
    }
    @Test
    void countsSpringMessagingConversionFailuresBeforeTheirFatalRejection() {
        ListenerExecutionFailedException failure = new ListenerExecutionFailedException(
                "conversion failed",
                new org.springframework.messaging.converter.MessageConversionException("malformed payload"),
                message()
        );

        assertThatThrownBy(() -> errorHandler.handleError(failure))
                .isInstanceOf(AmqpRejectAndDontRequeueException.class);

        assertThat(registry.get("anomaly.detector.points.rejected").counter().count()).isEqualTo(1.0d);
    }
    @Test
    void doesNotCountARecovererConversionRejectionTwice() {
        MessageConversionException conversionFailure = new MessageConversionException("malformed payload");
        assertThat(errorHandler.recordConversionFailure(conversionFailure)).isTrue();

        errorHandler.handleError(new AmqpRejectAndDontRequeueException(
                "Fatal message conversion failed",
                true,
                conversionFailure
        ));

        assertThat(registry.get("anomaly.detector.points.rejected").counter().count()).isEqualTo(1.0d);
    }



    @Test
    void doesNotCountNonConversionFailuresAsRejected() {
        ListenerExecutionFailedException failure = new ListenerExecutionFailedException(
                "processor failed",
                new IllegalStateException("unexpected"),
                message()
        );

        errorHandler.handleError(failure);

        assertThat(registry.get("anomaly.detector.points.rejected").counter().count()).isZero();
    }

    private Message message() {
        return new Message(new byte[0], new MessageProperties());
    }
}
