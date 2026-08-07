package com.emikaelsilveira.anomalydetector.producer.messaging;

import java.util.Map;

public final class RabbitTopology {

    public static final String METRICS_EXCHANGE = "metrics.exchange";
    public static final String METRICS_ROUTING_KEY = "metrics.datapoint";
    public static final String DATAPOINT_QUEUE = "metrics.datapoint.q";
    public static final String METRICS_DLX = "metrics.dlx";
    public static final String DATAPOINT_DLQ = "metrics.datapoint.dlq";

    private RabbitTopology() {
    }

    public static Map<String, Object> mainQueueArguments() {
        return Map.of(
                "x-dead-letter-exchange", METRICS_DLX,
                "x-dead-letter-routing-key", DATAPOINT_DLQ,
                "x-max-length", 10_000,
                "x-overflow", "reject-publish"
        );
    }
}
