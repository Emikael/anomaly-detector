package com.emikaelsilveira.anomalydetector.producer.messaging;

import java.time.Duration;
import java.util.Map;

import com.emikaelsilveira.anomalydetector.producer.contract.Datapoint;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.DefaultJacksonJavaTypeMapper;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
@Configuration(proxyBeanMethods = false)
public class RabbitConfiguration {

    /**
     * Logical wire type id, matching {@code contracts/datapoint.v1.schema.json}. The converter always
     * stamps a {@code __TypeId__} header; without an explicit mapping it would be the producer's
     * fully-qualified class name, putting an internal package on the wire and contradicting AD-05,
     * which makes the schema — not a shared Java class — the contract.
     */
    static final String DATAPOINT_TYPE_ID = "datapoint.v1";

    @Bean
    JacksonJsonMessageConverter jacksonJsonMessageConverter() {
        JacksonJsonMessageConverter converter = new JacksonJsonMessageConverter(
                JsonMapper.builder().findAndAddModules().build());
        DefaultJacksonJavaTypeMapper typeMapper = new DefaultJacksonJavaTypeMapper();
        typeMapper.setIdClassMapping(Map.of(DATAPOINT_TYPE_ID, Datapoint.class));
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
}
