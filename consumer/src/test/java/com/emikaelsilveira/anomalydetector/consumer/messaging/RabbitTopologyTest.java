package com.emikaelsilveira.anomalydetector.consumer.messaging;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitTopologyTest {

    @Test
    void pinsTheDurableTopologyLiteralsMirroredByTheOtherModule() {
        assertThat(RabbitTopology.METRICS_EXCHANGE).isEqualTo("metrics.exchange");
        assertThat(RabbitTopology.METRICS_ROUTING_KEY).isEqualTo("metrics.datapoint");
        assertThat(RabbitTopology.DATAPOINT_QUEUE).isEqualTo("metrics.datapoint.q");
        assertThat(RabbitTopology.METRICS_DLX).isEqualTo("metrics.dlx");
        assertThat(RabbitTopology.DATAPOINT_DLQ).isEqualTo("metrics.datapoint.dlq");
        assertThat(RabbitTopology.mainQueueArguments()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "x-dead-letter-exchange", "metrics.dlx",
                "x-dead-letter-routing-key", "metrics.datapoint.dlq",
                "x-max-length", 10_000,
                "x-overflow", "reject-publish"
        ));
    }
}
