package com.emikaelsilveira.anomalydetector.producer.contract;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Producer-side representation of the versioned datapoint JSON wire contract. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Datapoint(UUID id, long sequence, String metric, double value, Instant emittedAt) {
}
