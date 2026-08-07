package com.emikaelsilveira.anomalydetector.consumer.processing;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public final class EventTimeMonitor {

    private static final Duration MAXIMUM_DRIFT = Duration.ofSeconds(5);

    private final Clock processingClock;

    public EventTimeMonitor(Clock processingClock) {
        this.processingClock = Objects.requireNonNull(processingClock, "processingClock");
    }

    public boolean isDrifted(Instant emittedAt) {
        return drift(emittedAt).compareTo(MAXIMUM_DRIFT) > 0;
    }

    public Duration drift(Instant emittedAt) {
        return Duration.between(Objects.requireNonNull(emittedAt, "emittedAt"), processingClock.instant()).abs();
    }
}
