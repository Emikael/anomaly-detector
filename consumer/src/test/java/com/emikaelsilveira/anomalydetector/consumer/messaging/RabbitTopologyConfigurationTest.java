package com.emikaelsilveira.anomalydetector.consumer.messaging;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitTopologyConfigurationTest {

    private final RabbitTopologyConfiguration configuration = new RabbitTopologyConfiguration();

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
}
