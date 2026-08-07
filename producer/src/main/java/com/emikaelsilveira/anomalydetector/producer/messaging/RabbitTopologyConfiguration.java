package com.emikaelsilveira.anomalydetector.producer.messaging;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declares the durable topology, separately from the publishing concerns in
 * {@link RabbitConfiguration}.
 *
 * <p>Split out because this class is deliberately identical to the consumer's copy: either service
 * may start first, so both must be able to declare the full topology, and a divergence between the
 * two would surface as a broker {@code PRECONDITION_FAILED} at startup rather than as a compile
 * error. Keeping the duplicated half in its own file makes the pairing reviewable — the names it
 * uses are shared through {@link RabbitTopology}, but the declarations cannot be, because neither
 * module depends on the other (AD-05).
 */
@Configuration(proxyBeanMethods = false)
public class RabbitTopologyConfiguration {

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
