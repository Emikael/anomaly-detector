package com.emikaelsilveira.anomalydetector.consumer.messaging;

import com.emikaelsilveira.anomalydetector.consumer.metrics.ConsumerMetrics;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.listener.ConditionalRejectingErrorHandler;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.util.ErrorHandler;

@RequiredArgsConstructor
public final class FatalMessageErrorHandler implements ErrorHandler {

    private final @NonNull ConsumerMetrics metrics;
    private final ConditionalRejectingErrorHandler delegate = rejectingErrorHandler();

    /**
     * {@code rejectManual} is the whole point of delegating here: the container acknowledges
     * manually, so without it a fatal conversion failure would be logged and then left unacked,
     * stalling the queue behind an unredeliverable message.
     */
    private static ConditionalRejectingErrorHandler rejectingErrorHandler() {
        ConditionalRejectingErrorHandler handler = new ConditionalRejectingErrorHandler();
        handler.setRejectManual(true);
        return handler;
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
