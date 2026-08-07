package com.emikaelsilveira.anomalydetector.producer.contract;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Datapoint(UUID id, long sequence, String metric, double value, Instant emittedAt) {
}
