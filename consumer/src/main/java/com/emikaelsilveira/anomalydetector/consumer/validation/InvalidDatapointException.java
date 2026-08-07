package com.emikaelsilveira.anomalydetector.consumer.validation;

public final class InvalidDatapointException extends RuntimeException {

    public InvalidDatapointException(String field) {
        super("Invalid datapoint field: " + field);
    }
}
