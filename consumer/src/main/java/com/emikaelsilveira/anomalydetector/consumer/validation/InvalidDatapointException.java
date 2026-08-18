package com.emikaelsilveira.anomalydetector.consumer.validation;

/** Signals a terminal datapoint contract violation while retaining the offending field name. */
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
