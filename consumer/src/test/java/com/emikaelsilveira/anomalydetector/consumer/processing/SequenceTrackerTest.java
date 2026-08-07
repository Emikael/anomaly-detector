package com.emikaelsilveira.anomalydetector.consumer.processing;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SequenceTrackerTest {

    @Test
    void observesTheFirstPointThenContiguousArrivalOrder() {
        SequenceTracker tracker = new SequenceTracker();

        assertThat(tracker.observe(10L)).isEqualTo(new SequenceObservation(SequenceObservation.Status.FIRST, 10L, 10L));
        assertThat(tracker.observe(11L)).isEqualTo(new SequenceObservation(SequenceObservation.Status.IN_ORDER, 11L, 11L));
    }

    @Test
    void warnsAboutGapsWithoutDroppingThePoint() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.observe(10L);

        assertThat(tracker.observe(13L)).isEqualTo(new SequenceObservation(SequenceObservation.Status.GAP, 11L, 13L));
    }

    @Test
    void warnsAboutOutOfOrderPointsWithoutRollingBackTheMaximumObservedSequence() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.observe(10L);
        tracker.observe(13L);

        assertThat(tracker.observe(11L)).isEqualTo(new SequenceObservation(SequenceObservation.Status.OUT_OF_ORDER, 14L, 11L));
        assertThat(tracker.observe(14L)).isEqualTo(new SequenceObservation(SequenceObservation.Status.IN_ORDER, 14L, 14L));
    }
}
