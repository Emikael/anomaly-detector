package com.emikaelsilveira.anomalydetector.producer.messaging;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import com.emikaelsilveira.anomalydetector.producer.contract.Datapoint;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.util.backoff.ExponentialBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RabbitConfigurationTest {

    private final RabbitConfiguration configuration = new RabbitConfiguration();

    @Test
    void configuresJacksonThreeJsonConversion() {
        JacksonJsonMessageConverter converter = configuration.jacksonJsonMessageConverter();
        Datapoint datapoint = new Datapoint(
                java.util.UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93"),
                1041,
                "sensor.temperature",
                100.4213,
                Instant.parse("2026-08-05T14:22:07.361Z")
        );

        Message message = converter.toMessage(datapoint, new MessageProperties());

        assertThat(message.getMessageProperties().getContentType()).isEqualTo(MessageProperties.CONTENT_TYPE_JSON);
        assertThat(converter.fromMessage(message)).isEqualTo(datapoint);
    }

    @Test
    void stampsALogicalTypeIdRatherThanTheProducerClassName() {
        JacksonJsonMessageConverter converter = configuration.jacksonJsonMessageConverter();

        Message message = converter.toMessage(datapoint(), new MessageProperties());

        Object typeId = message.getMessageProperties().getHeaders().get("__TypeId__");
        assertThat(typeId).isEqualTo(RabbitConfiguration.DATAPOINT_TYPE_ID);
        // AD-05: the schema is the contract, so no producer package may travel on the wire.
        assertThat(String.valueOf(typeId)).doesNotContain("com.emikaelsilveira");
    }

    private Datapoint datapoint() {
        return new Datapoint(
                java.util.UUID.fromString("0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93"),
                1041,
                "sensor.temperature",
                100.4213,
                Instant.parse("2026-08-05T14:22:07.361Z")
        );
    }

    @Test
    void retriesConnectionFailuresExactlyThreeTimesWithTheConfiguredBackoff() {
        RetryTemplate retryTemplate = configuration.connectionRetryTemplate();
        ExponentialBackOff backOff = (ExponentialBackOff) retryTemplate.getRetryPolicy().getBackOff();
        AtomicInteger attempts = new AtomicInteger();

        assertThat(backOff.getInitialInterval()).isEqualTo(250);
        assertThat(backOff.getMultiplier()).isEqualTo(2.0);
        assertThat(backOff.getMaxInterval()).isEqualTo(2_000);
        assertThat(retryTemplate.getRetryPolicy().getTimeout()).isEqualTo(Duration.ZERO);
        assertThatThrownBy(() -> retryTemplate.invoke(() -> {
            attempts.incrementAndGet();
            throw new AmqpConnectException("broker unavailable", new IllegalStateException());
        })).isInstanceOf(AmqpConnectException.class);
        assertThat(attempts).hasValue(3);
    }
    @Test
    void retriesRabbitTemplateConnectionFailuresExactlyThreeTimes() {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        AmqpConnectException failure = new AmqpConnectException("broker unavailable", new IllegalStateException());
        when(connectionFactory.createConnection()).thenThrow(failure);
        RabbitTemplate template = configuration.rabbitTemplate(
                connectionFactory,
                configuration.jacksonJsonMessageConverter(),
                configuration.connectionRetryTemplate()
        );

        assertThatThrownBy(() -> template.convertAndSend(RabbitTopology.METRICS_EXCHANGE, "unavailable", "body"))
                .isSameAs(failure);

        verify(connectionFactory, times(3)).createConnection();
    }


    @Test
    void configuresMandatoryPublishingWithTheJacksonConverterAndCoreRetry() {
        JacksonJsonMessageConverter converter = configuration.jacksonJsonMessageConverter();
        RetryTemplate retryTemplate = configuration.connectionRetryTemplate();
        RabbitTemplate template = configuration.rabbitTemplate(mock(ConnectionFactory.class), converter, retryTemplate);

        assertThat(template.getMessageConverter()).isSameAs(converter);
        assertThat(template.isMandatoryFor(new Message(new byte[0], new MessageProperties()))).isTrue();
    }
}
