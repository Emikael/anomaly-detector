package com.emikaelsilveira.anomalydetector.producer.messaging;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.DefaultJacksonJavaTypeMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
public class RabbitConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitConfiguration.class);

    @Bean
    JacksonJsonMessageConverter jacksonJsonMessageConverter() {
        JacksonJsonMessageConverter converter = new JacksonJsonMessageConverter(
                JsonMapper.builder().findAndAddModules().build());
        DefaultJacksonJavaTypeMapper typeMapper = new DefaultJacksonJavaTypeMapper();
        typeMapper.setTrustedPackages("com.emikaelsilveira.anomalydetector.producer.contract");
        converter.setJavaTypeMapper(typeMapper);
        return converter;
    }

    @Bean
    RetryTemplate connectionRetryTemplate() {
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .includes(AmqpConnectException.class)
                .maxRetries(2)
                .delay(Duration.ofMillis(250))
                .multiplier(2.0)
                .maxDelay(Duration.ofSeconds(2))
                .build();
        return new RetryTemplate(retryPolicy);
    }

    @Bean
    RabbitTemplate rabbitTemplate(
            ConnectionFactory connectionFactory,
            JacksonJsonMessageConverter converter,
            RetryTemplate connectionRetryTemplate
    ) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(converter);
        template.setMandatory(true);
        template.setRetryTemplate(connectionRetryTemplate);
        template.setConfirmCallback((correlation, acknowledged, cause) -> {
            if (!acknowledged) {
                LOGGER.error("RabbitMQ publisher confirm nack correlationId={} reason={}",
                        correlation == null ? null : correlation.getId(), cause);
            }
        });
        template.setReturnsCallback(returned -> LOGGER.error(
                "RabbitMQ returned unroutable message id={} replyCode={} replyText={} exchange={} routingKey={}",
                returned.getMessage().getMessageProperties().getMessageId(),
                returned.getReplyCode(),
                returned.getReplyText(),
                returned.getExchange(),
                returned.getRoutingKey()
        ));
        return template;
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
}
