package com.emikaelsilveira.anomalydetector.consumer.validation;

public final class InvalidDatapointException extends RuntimeException {

    private final String field;

    public InvalidDatapointException(String field) {
        super("Invalid datapoint field: " + field);
        this.field = field;
    }

    /** The offending field, so a rejection can be logged without parsing the message. */
    public String field() {
        return field;
    }
}
