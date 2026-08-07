package com.emikaelsilveira.anomalydetector.consumer.messaging;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import com.emikaelsilveira.anomalydetector.consumer.contract.Datapoint;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.ContainerCustomizer;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JacksonJavaTypeMapper;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.backoff.ExponentialBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class RabbitConfigurationTest {

    private final RabbitConfiguration configuration = new RabbitConfiguration();

    @Test
    void declaresTheSameDurableTopologyAsTheProducer() {
        DirectExchange metricsExchange = configuration.metricsExchange();
        DirectExchange metricsDlx = configuration.metricsDlx();
        Queue datapointQueue = configuration.datapointQueue();
        Queue datapointDlq = configuration.datapointDlq();
        Binding datapointBinding = configuration.datapointBinding(datapointQueue, metricsExchange);
        Binding dlqBinding = configuration.datapointDlqBinding(datapointDlq, metricsDlx);

        assertThat(metricsExchange.isDurable()).isTrue();
        assertThat(metricsExchange.isAutoDelete()).isFalse();
        assertThat(metricsDlx.isDurable()).isTrue();
        assertThat(metricsDlx.isAutoDelete()).isFalse();
        assertThat(datapointQueue.isDurable()).isTrue();
        assertThat(datapointQueue.getArguments()).isEqualTo(RabbitTopology.mainQueueArguments());
        assertThat(datapointDlq.isDurable()).isTrue();
        assertThat(datapointBinding.getRoutingKey()).isEqualTo(RabbitTopology.METRICS_ROUTING_KEY);
        assertThat(dlqBinding.getRoutingKey()).isEqualTo(RabbitTopology.DATAPOINT_DLQ);
    }

    @Test
    void configuresStrictInferredJacksonThreeConversion() {
        JacksonJsonMessageConverter converter = configuration.jacksonJsonMessageConverter();
        assertThat(converter.fromMessage(datapointMessage("""
                {"id":"0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93","sequence":1,"metric":"sensor.temperature","value":1.0,"emittedAt":"2026-08-05T14:22:03.114Z","future":"ignored"}
                """))).isInstanceOf(Datapoint.class);
        assertThatThrownBy(() -> converter.fromMessage(datapointMessage("""
                {"id":"0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93","metric":"sensor.temperature","value":1.0,"emittedAt":"2026-08-05T14:22:03.114Z"}
                """))).isInstanceOf(MessageConversionException.class);
        assertThatThrownBy(() -> converter.fromMessage(datapointMessage("""
                {"id":"0f3a9c1e-6b7d-4a2f-9c11-8de4b5a70c93","sequence":null,"metric":"sensor.temperature","value":1.0,"emittedAt":"2026-08-05T14:22:03.114Z"}
                """))).isInstanceOf(MessageConversionException.class);
    }

    @Test
    void configuresOneManualAckListenerWithSinglePrefetchAndThirtySecondDrain() {
        SimpleRabbitListenerContainerFactory factory = factoryWithConfirmedRecovery();

        assertThat(ReflectionTestUtils.getField(factory, "acknowledgeMode")).isEqualTo(AcknowledgeMode.MANUAL);
        assertThat(ReflectionTestUtils.getField(factory, "prefetchCount")).isEqualTo(1);
        assertThat(ReflectionTestUtils.getField(factory, "concurrentConsumers")).isEqualTo(1);
        assertThat(ReflectionTestUtils.getField(factory, "maxConcurrentConsumers")).isEqualTo(1);
        assertThat(ReflectionTestUtils.getField(factory, "defaultRequeueRejected")).isEqualTo(false);
        assertThat(ReflectionTestUtils.getField(factory, "enforceImmediateAckForManual")).isEqualTo(true);
        ContainerCustomizer<SimpleMessageListenerContainer> customizer =
                (ContainerCustomizer<SimpleMessageListenerContainer>) ReflectionTestUtils.getField(factory, "containerCustomizer");
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        customizer.configure(container);
        assertThat(ReflectionTestUtils.getField(container, "shutdownTimeout")).isEqualTo(30_000L);
    }

    @Test
    void retriesUnexpectedProcessingFailuresThreeTimesWithThePlannedBackoff() {
        RabbitTemplate recoveryTemplate = confirmedRecoveryTemplate();
        SimpleRabbitListenerContainerFactory factory = configuration.rabbitListenerContainerFactory(
                mock(ConnectionFactory.class),
                configuration.jacksonJsonMessageConverter(),
                new AcknowledgingRepublishMessageRecoverer(recoveryTemplate)
        );
        AtomicInteger attempts = new AtomicInteger();
        ProxyFactory proxyFactory = new ProxyFactory(new AlwaysFailingHandler(attempts));
        proxyFactory.addAdvice(factory.getAdviceChain()[0]);
        AlwaysFailingHandler handler = (AlwaysFailingHandler) proxyFactory.getProxy();

        assertThatThrownBy(() -> handler.process(mock(com.rabbitmq.client.Channel.class), message("{}")))
                .isInstanceOf(ImmediateAcknowledgeAmqpException.class);
        assertThat(attempts).hasValue(3);
    }

    @Test
    void configuresUnlimitedBrokerRecoveryWithThePlannedExponentialBackoff() {
        SimpleRabbitListenerContainerFactory factory = factoryWithConfirmedRecovery();

        ExponentialBackOff recoveryBackOff = (ExponentialBackOff) ReflectionTestUtils.getField(factory, "recoveryBackOff");
        assertThat(recoveryBackOff.getInitialInterval()).isEqualTo(1_000L);
        assertThat(recoveryBackOff.getMultiplier()).isEqualTo(2.0d);
        assertThat(recoveryBackOff.getMaxInterval()).isEqualTo(10_000L);
        assertThat(recoveryBackOff.getMaxElapsedTime()).isEqualTo(Long.MAX_VALUE);
        assertThat(recoveryBackOff.getMaxAttempts()).isEqualTo(Long.MAX_VALUE);
    }

    private SimpleRabbitListenerContainerFactory factoryWithConfirmedRecovery() {
        return configuration.rabbitListenerContainerFactory(
                mock(ConnectionFactory.class),
                configuration.jacksonJsonMessageConverter(),
                new AcknowledgingRepublishMessageRecoverer(confirmedRecoveryTemplate())
        );
    }

    private RabbitTemplate confirmedRecoveryTemplate() {
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
        return template;
    }

    private Message message(String body) {
        MessageProperties properties = new MessageProperties();
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        return new Message(body.getBytes(StandardCharsets.UTF_8), properties);
    }

    private Message datapointMessage(String body) {
        Message message = message(body);
        message.getMessageProperties().setInferredArgumentType(Datapoint.class);
        return message;
    }

    static class AlwaysFailingHandler {

        private final AtomicInteger attempts;

        AlwaysFailingHandler(AtomicInteger attempts) {
            this.attempts = attempts;
        }

        public void process(com.rabbitmq.client.Channel channel, Message message) {
            attempts.incrementAndGet();
            throw new AmqpException("processing failed");
        }
    }
}
