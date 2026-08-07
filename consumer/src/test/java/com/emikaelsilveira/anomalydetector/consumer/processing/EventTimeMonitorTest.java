package com.emikaelsilveira.anomalydetector.consumer.processing;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventTimeMonitorTest {

    private static final Instant PROCESSING_TIME = Instant.parse("2026-08-05T14:22:07.361Z");

    @Test
    void exactlyFiveSecondsBehindDoesNotWarn() {
        EventTimeMonitor monitor = monitor();

        assertThat(monitor.isExcessive(monitor.drift(PROCESSING_TIME.minusSeconds(5)))).isFalse();
    }

    @Test
    void exactlyFiveSecondsAheadDoesNotWarn() {
        EventTimeMonitor monitor = monitor();

        assertThat(monitor.isExcessive(monitor.drift(PROCESSING_TIME.plusSeconds(5)))).isFalse();
    }

    @Test
    void moreThanFiveSecondsBehindWarnsWithAnAbsoluteDrift() {
        EventTimeMonitor monitor = monitor();

        Duration drift = monitor.drift(PROCESSING_TIME.minusSeconds(6));

        assertThat(drift).isEqualTo(Duration.ofSeconds(6));
        assertThat(monitor.isExcessive(drift)).isTrue();
    }

    @Test
    void moreThanFiveSecondsAheadWarnsWithAnAbsoluteDrift() {
        EventTimeMonitor monitor = monitor();

        Duration drift = monitor.drift(PROCESSING_TIME.plusSeconds(6));

        assertThat(drift).isEqualTo(Duration.ofSeconds(6));
        assertThat(monitor.isExcessive(drift)).isTrue();
    }

    private EventTimeMonitor monitor() {
        return new EventTimeMonitor(Clock.fixed(PROCESSING_TIME, ZoneOffset.UTC));
    }
}
