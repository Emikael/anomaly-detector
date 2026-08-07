package com.emikaelsilveira.anomalydetector.consumer.messaging;

import java.util.Objects;

import com.emikaelsilveira.anomalydetector.consumer.metrics.ConsumerMetrics;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.listener.ConditionalRejectingErrorHandler;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.util.ErrorHandler;

public final class FatalMessageErrorHandler implements ErrorHandler {

    private final ConsumerMetrics metrics;
    private final ConditionalRejectingErrorHandler delegate = new ConditionalRejectingErrorHandler();

    public FatalMessageErrorHandler(ConsumerMetrics metrics) {
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        delegate.setRejectManual(true);
    }

    @Override
    public void handleError(Throwable throwable) {
        if (!(throwable instanceof AmqpRejectAndDontRequeueException)) {
            recordConversionFailure(throwable);
        }
        delegate.handleError(throwable);
    }

    boolean recordConversionFailure(Throwable throwable) {
        if (!isConversionFailure(throwable)) {
            return false;
        }
        metrics.recordRejected();
        return true;
    }

    static boolean isConversionFailure(Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause instanceof MessageConversionException
                    || cause instanceof org.springframework.messaging.converter.MessageConversionException) {
                return true;
            }
        }
        return false;
    }
}
