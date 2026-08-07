package com.emikaelsilveira.anomalydetector.consumer.processing;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/** Compares event time against processing time so a backlog replay cannot be mistaken for live data. */
@RequiredArgsConstructor
public final class EventTimeMonitor {

    private static final Duration MAXIMUM_DRIFT = Duration.ofSeconds(5);

    private final @NonNull Clock processingClock;

    /** Absolute distance between event time and processing time, sampling the clock exactly once. */
    public Duration drift(@NonNull Instant emittedAt) {
        return Duration.between(emittedAt, processingClock.instant()).abs();
    }

    public boolean isExcessive(@NonNull Duration drift) {
        return drift.compareTo(MAXIMUM_DRIFT) > 0;
    }
}
