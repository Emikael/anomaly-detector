package com.emikaelsilveira.anomalydetector.consumer.messaging;
import java.time.Duration;


import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import com.emikaelsilveira.anomalydetector.consumer.metrics.ConsumerMetrics;
import com.emikaelsilveira.anomalydetector.consumer.processing.DatapointProcessor;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.amqp.support.converter.DefaultJacksonJavaTypeMapper;
import org.springframework.amqp.support.converter.JacksonJavaTypeMapper;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.backoff.ExponentialBackOff;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
@EnableRabbit
public class RabbitConfiguration {

    private static final long SHUTDOWN_TIMEOUT_MILLIS = 30_000L;

    @Bean
    JacksonJsonMessageConverter jacksonJsonMessageConverter() {
        JacksonJsonMessageConverter converter = new JacksonJsonMessageConverter(JsonMapper.builder()
                .findAndAddModules()
                .enable(
                        DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES,
                        DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES
                )
                .build());
        DefaultJacksonJavaTypeMapper typeMapper = new DefaultJacksonJavaTypeMapper();
        typeMapper.setTrustedPackages("com.emikaelsilveira.anomalydetector.consumer.contract");
        typeMapper.setTypePrecedence(JacksonJavaTypeMapper.TypePrecedence.INFERRED);
        converter.setJavaTypeMapper(typeMapper);
        return converter;
    }

    @Bean
    AcknowledgingRepublishMessageRecoverer acknowledgingRepublishMessageRecoverer(
            RabbitTemplate rabbitTemplate,
            FatalMessageErrorHandler errorHandler
    ) {
        return new AcknowledgingRepublishMessageRecoverer(rabbitTemplate, errorHandler);
    }

    @Bean
    FatalMessageErrorHandler fatalMessageErrorHandler(ConsumerMetrics metrics) {
        return new FatalMessageErrorHandler(metrics);
    }

    @Bean
    DatapointListener datapointListener(DatapointProcessor processor, ConsumerMetrics metrics) {
        return new DatapointListener(processor, metrics);
    }

    @Bean
    SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            JacksonJsonMessageConverter converter,
            AcknowledgingRepublishMessageRecoverer recoverer,
            FatalMessageErrorHandler errorHandler
    ) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(converter);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setConcurrentConsumers(1);
        factory.setMaxConcurrentConsumers(1);
        factory.setPrefetchCount(1);
        factory.setDefaultRequeueRejected(false);
        factory.setEnforceImmediateAckForManual(true);
        factory.setContainerCustomizer(container -> container.setShutdownTimeout(SHUTDOWN_TIMEOUT_MILLIS));
        factory.setErrorHandler(errorHandler);
        factory.setAdviceChain(RetryInterceptorBuilder.stateless()
                .retryPolicy(processingRetryPolicy())
                .recoverer(recoverer)
                .build());
        factory.setRecoveryBackOff(brokerRecoveryBackOff());
        return factory;
    }

    @Bean
    DirectExchange metricsExchange() {
        return new DirectExchange(RabbitTopology.METRICS_EXCHANGE, true, false);
    }

    @Bean
    DirectExchange metricsDlx() {
        return new DirectExchange(RabbitTopology.METRICS_DLX, true, false);
    }

    @Bean
    Queue datapointQueue() {
        return new Queue(RabbitTopology.DATAPOINT_QUEUE, true, false, false, RabbitTopology.mainQueueArguments());
    }

    @Bean
    Queue datapointDlq() {
        return new Queue(RabbitTopology.DATAPOINT_DLQ, true);
    }

    @Bean
    Binding datapointBinding(
            @Qualifier("datapointQueue") Queue queue,
            @Qualifier("metricsExchange") DirectExchange exchange
    ) {
        return BindingBuilder.bind(queue).to(exchange).with(RabbitTopology.METRICS_ROUTING_KEY);
    }

    @Bean
    Binding datapointDlqBinding(
            @Qualifier("datapointDlq") Queue queue,
            @Qualifier("metricsDlx") DirectExchange exchange
    ) {
        return BindingBuilder.bind(queue).to(exchange).with(RabbitTopology.DATAPOINT_DLQ);
    }

    private RetryPolicy processingRetryPolicy() {
        return RetryPolicy.builder()
                .maxRetries(2)
                .delay(Duration.ofMillis(250L))
                .multiplier(2.0d)
                .maxDelay(Duration.ofSeconds(2L))
                .predicate(throwable -> !FatalMessageErrorHandler.isConversionFailure(throwable))
                .build();
    }


    private ExponentialBackOff brokerRecoveryBackOff() {
        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0d);
        backOff.setMaxInterval(10_000L);
        backOff.setMaxElapsedTime(Long.MAX_VALUE);
        backOff.setMaxAttempts(Long.MAX_VALUE);
        return backOff;
    }
}
