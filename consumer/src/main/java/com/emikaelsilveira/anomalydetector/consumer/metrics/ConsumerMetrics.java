package com.emikaelsilveira.anomalydetector.consumer.metrics;

import java.time.Clock;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.emikaelsilveira.anomalydetector.consumer.detection.DetectionResult;
import com.emikaelsilveira.anomalydetector.consumer.detection.DetectionStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConsumerMetrics {

    static final String POINTS_PROCESSED = "anomaly.detector.points.processed";
    static final String POINTS_ANOMALIES = "anomaly.detector.points.anomalies";
    static final String POINTS_REJECTED = "anomaly.detector.points.rejected";
    static final String POINTS_DUPLICATES = "anomaly.detector.points.duplicates";
    static final String WINDOW_OCCUPANCY = "anomaly.detector.window.occupancy";
    static final String Z_SCORE = "anomaly.detector.zscore";
    static final String PROCESSING = "anomaly.detector.processing";

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerMetrics.class);
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendInstant(3)
            .toFormatter();

    private final Clock processingClock;
    private final int summaryEvery;
    private final int capacity;
    private final Counter processed;
    private final Counter anomalies;
    private final Counter rejected;
    private final Counter duplicates;
    private final AtomicInteger windowOccupancy = new AtomicInteger();
    private final DistributionSummary zScores;
    private final Timer processing;

    private long processedCount;
    private long anomalyCount;
    private long rejectedCount;
    private long duplicateCount;
    private long finiteZScoreCount;
    private double meanZScore;

    public ConsumerMetrics(MeterRegistry meterRegistry, Clock processingClock, int summaryEvery, int capacity) {
        MeterRegistry requiredRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
        this.processingClock = Objects.requireNonNull(processingClock, "processingClock");
        if (summaryEvery < 1) {
            throw new IllegalArgumentException("summaryEvery must be positive");
        }
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.summaryEvery = summaryEvery;
        this.capacity = capacity;
        processed = Counter.builder(POINTS_PROCESSED).register(requiredRegistry);
        anomalies = Counter.builder(POINTS_ANOMALIES).register(requiredRegistry);
        rejected = Counter.builder(POINTS_REJECTED).register(requiredRegistry);
        duplicates = Counter.builder(POINTS_DUPLICATES).register(requiredRegistry);
        Gauge.builder(WINDOW_OCCUPANCY, windowOccupancy, AtomicInteger::get).register(requiredRegistry);
        zScores = DistributionSummary.builder(Z_SCORE).register(requiredRegistry);
        processing = Timer.builder(PROCESSING)
                .publishPercentiles(0.99d)
                .register(requiredRegistry);
    }

    public void recordProcessed(DetectionResult result, Duration elapsed) {
        DetectionResult requiredResult = Objects.requireNonNull(result, "result");
        processing.record(Objects.requireNonNull(elapsed, "elapsed"));
        processed.increment();
        processedCount++;
        windowOccupancy.set(requiredResult.samples());
        if (requiredResult.status() == DetectionStatus.ANOMALY) {
            anomalies.increment();
            anomalyCount++;
        }
        requiredResult.zScore().ifPresent(this::recordFiniteZScore);
        if (processedCount % summaryEvery == 0) {
            logSummary();
        }
    }

    public void recordRejected() {
        rejected.increment();
        rejectedCount++;
    }

    public void recordDuplicate() {
        duplicates.increment();
        duplicateCount++;
    }

    double meanZScore() {
        return finiteZScoreCount == 0 ? 0.0d : meanZScore;
    }

    private void recordFiniteZScore(double zScore) {
        if (!Double.isFinite(zScore)) {
            return;
        }
        zScores.record(zScore);
        finiteZScoreCount++;
        meanZScore += (zScore - meanZScore) / finiteZScoreCount;
    }

    private void logSummary() {
        LOGGER.atInfo().log(String.format(
                Locale.ROOT,
                "[%s] SUMMARY | processed=%d anomalies=%d (%.2f%%) rejected=%d duplicates=%d window=%d/%d meanZ=%.2f p99ProcessingMicros=%d",
                TIMESTAMP_FORMATTER.format(processingClock.instant()),
                processedCount,
                anomalyCount,
                anomalyPercentage(),
                rejectedCount,
                duplicateCount,
                windowOccupancy.get(),
                capacity,
                meanZScore(),
                p99ProcessingMicros()
        ));
    }

    private double anomalyPercentage() {
        return processedCount == 0 ? 0.0d : (100.0d * anomalyCount) / processedCount;
    }

    private long p99ProcessingMicros() {
        for (ValueAtPercentile percentile : processing.takeSnapshot().percentileValues()) {
            if (Double.compare(percentile.percentile(), 0.99d) == 0) {
                double microseconds = percentile.value(TimeUnit.MICROSECONDS);
                return Double.isFinite(microseconds) ? Math.max(0L, Math.round(microseconds)) : 0L;
            }
        }
        return 0L;
    }
}
